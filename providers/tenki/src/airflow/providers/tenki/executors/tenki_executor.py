# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
Tenki Sandbox Executor.

Each Airflow task is delegated to a dedicated, ephemeral Tenki sandbox (an isolated
micro-VM) provisioned through the ``tenki-sandbox`` SDK. The scheduler hands the executor
an ``ExecuteTask`` workload; the executor serializes it into a Task SDK command and, in a
bounded worker-thread pool, creates a sandbox and runs that command inside it via the SDK's
blocking ``Sandbox.exec``. When the command returns, the task is marked succeeded or failed
based on its exit status and the sandbox is terminated.

Why a thread pool: the ``tenki-sandbox`` SDK exposes only a blocking ``exec``/``wait`` (a
timed ``wait`` *kills* the process), and a process handle cannot be recovered from a sandbox
id after a reconnect. A fire-and-forget "poll by id" model (like the Kubernetes/ECS
executors) therefore isn't supported by the SDK, so each task's blocking call is run in a
worker thread whose pool size is bounded by ``parallelism``. As a consequence, task adoption
across scheduler restarts is not supported (as with the LocalExecutor); orphaned sandboxes
are bounded by their ``idle_timeout``/``max_duration`` and are terminated when the command
returns or on ``terminate``.

Individual tasks can request a specific image, CPU, memory, environment, etc. through their
``executor_config``, which is merged on top of the defaults in the ``[tenki_executor]``
configuration section.
"""

from __future__ import annotations

import contextlib
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from functools import cached_property
from typing import TYPE_CHECKING, TypeAlias

from airflow.executors.base_executor import BaseExecutor
from airflow.providers.tenki.executors.utils import (
    CONFIG_DEFAULTS,
    CONFIG_GROUP_NAME,
    RunningWorkload,
    SandboxHolder,
    TenkiExecutorException,
    TenkiQueuedTask,
    calculate_next_attempt_delay,
)
from airflow.providers.tenki.hooks.tenki import TenkiSandboxHook
from airflow.providers.tenki.version_compat import AIRFLOW_V_3_0_PLUS, AIRFLOW_V_3_3_PLUS
from airflow.utils.timezone import utcnow

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session
    from tenki_sandbox import Client, CommandResult

    from airflow.configuration import AirflowConfigParser
    from airflow.executors import workloads
    from airflow.executors.base_executor import ExecutorConf
    from airflow.models.taskinstance import TaskInstance, TaskInstanceKey
    from airflow.providers.tenki.executors.utils import CommandType, ExecutorConfigType

    if AIRFLOW_V_3_3_PLUS:
        from airflow.executors.workloads.types import WorkloadKey as _WorkloadKey

        WorkloadKey: TypeAlias = _WorkloadKey
    else:
        WorkloadKey: TypeAlias = TaskInstanceKey  # type: ignore[no-redef, misc]

    ConfSource: TypeAlias = AirflowConfigParser | ExecutorConf


class TenkiExecutor(BaseExecutor):
    """Runs each Airflow task inside a dedicated Tenki sandbox via the ``tenki-sandbox`` SDK."""

    supports_multi_team: bool = True

    if AIRFLOW_V_3_3_PLUS:
        supports_callbacks: bool = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pending_workloads: deque[TenkiQueuedTask] = deque()
        self.active_workloads: dict[WorkloadKey, RunningWorkload] = {}
        self._pool: ThreadPoolExecutor | None = None

        # Backwards-compat with Airflow versions predating the multi-team ExecutorConf
        # and the always-present team_name attribute.
        if not hasattr(self, "conf"):
            from airflow.configuration import conf

            self.conf = conf
        if not hasattr(self, "team_name"):
            self.team_name = None

        self.max_run_sandbox_attempts = int(
            self.conf.get(
                CONFIG_GROUP_NAME,
                "max_run_sandbox_attempts",
                fallback=CONFIG_DEFAULTS["max_run_sandbox_attempts"],
            )
        )
        self.sandbox_kwargs = self._load_sandbox_kwargs()

    @property
    def hook(self) -> TenkiSandboxHook:
        return TenkiSandboxHook(
            conn_id=self.conf.get(CONFIG_GROUP_NAME, "conn_id", fallback=CONFIG_DEFAULTS["conn_id"]),
            auth_token=self.conf.get(CONFIG_GROUP_NAME, "auth_token", fallback=None),
            base_url=self.conf.get(CONFIG_GROUP_NAME, "base_url", fallback=None),
            gateway_url=self.conf.get(CONFIG_GROUP_NAME, "gateway_url", fallback=None),
        )

    @cached_property
    def client(self) -> Client:
        return self.hook.conn

    def _load_sandbox_kwargs(self) -> dict:
        from airflow.providers.tenki.executors.tenki_executor_config import build_sandbox_kwargs

        return build_sandbox_kwargs(self.conf)

    def start(self):
        """Create the worker pool and optionally verify connectivity to the Tenki service."""
        self._pool = ThreadPoolExecutor(max_workers=self.parallelism, thread_name_prefix="tenki-sandbox")
        # Build the shared client in the main thread so worker threads never race on the
        # cached_property initialization when the health check below is disabled.
        _ = self.client
        check_health = self.conf.getboolean(CONFIG_GROUP_NAME, "check_health_on_startup", fallback=True)
        if check_health:
            self.log.info("Starting Tenki Executor and checking service health...")
            self.check_health()

    def check_health(self):
        """Authenticate against the Tenki service; raise if unreachable or unauthorized."""
        try:
            self.client.who_am_i()
        except Exception as err:
            self.log.error(
                "Tenki Executor health check failed. The executor cannot run tasks until this is fixed."
            )
            raise TenkiExecutorException(f"Tenki health check failed: {err}") from err
        self.log.info("Tenki Executor health check succeeded.")

    # TODO: Remove this once the minimum supported version is 3.3+ and defer to BaseExecutor.queue_workload.
    def queue_workload(self, workload: workloads.All, session: Session | None) -> None:
        from airflow.executors import workloads

        if isinstance(workload, workloads.ExecuteTask):
            self.queued_tasks[workload.ti.key] = workload
            return
        if AIRFLOW_V_3_3_PLUS and isinstance(workload, workloads.ExecuteCallback):
            self.queued_callbacks[workload.callback.key] = workload
            return
        raise TenkiExecutorException(f"{type(self)} cannot handle workloads of type {type(workload)}")

    def _process_workloads(self, workload_items: Sequence[workloads.All]) -> None:
        from airflow.executors import workloads

        for workload in workload_items:
            if isinstance(workload, workloads.ExecuteTask):
                key = workload.ti.key
                queue = workload.ti.queue
                executor_config = workload.ti.executor_config or {}
                del self.queued_tasks[key]
                self.execute_async(key=key, command=[workload], queue=queue, executor_config=executor_config)
                self.running.add(key)
            elif AIRFLOW_V_3_3_PLUS and isinstance(workload, workloads.ExecuteCallback):
                callback_key = workload.callback.key
                del self.queued_callbacks[callback_key]
                self.execute_async(key=callback_key, command=[workload], queue=None)
                self.running.add(callback_key)
            else:
                raise TenkiExecutorException(f"{type(self)} cannot handle workloads of type {type(workload)}")

    def execute_async(self, key: WorkloadKey, command: CommandType, queue=None, executor_config=None):
        """Serialize the workload and queue it to be launched on the next sync."""
        if executor_config and "command" in executor_config:
            raise ValueError('executor_config must never override the sandbox "command"')
        if len(command) == 1:
            from airflow.executors import workloads

            if isinstance(command[0], workloads.ExecuteTask) or (
                AIRFLOW_V_3_3_PLUS and isinstance(command[0], workloads.ExecuteCallback)
            ):
                command = self._serialize_workload_to_command(command[0])
            else:
                raise TenkiExecutorException(
                    f"TenkiExecutor doesn't know how to handle workload of type: {type(command[0])}"
                )

        self.pending_workloads.append(
            TenkiQueuedTask(key, command, queue, executor_config or {}, 1, utcnow())
        )

    def sync(self):
        try:
            self._collect_finished_workloads()
            self._launch_pending_workloads()
            self._report_running_state()
        except Exception:
            # Never let a sync error bubble up and kill the scheduler process.
            self.log.exception("Failed to sync %s", self.__class__.__name__)

    def _launch_pending_workloads(self):
        """Submit each pending workload whose backoff window has elapsed to the worker pool."""
        if self._pool is None:
            self.start()
        for _ in range(len(self.pending_workloads)):
            task = self.pending_workloads.popleft()
            if utcnow() < task.next_attempt_time:
                self.pending_workloads.append(task)
                continue
            holder = SandboxHolder()
            future = self._pool.submit(  # type: ignore[union-attr]
                self._run_workload_in_sandbox, task.command, task.executor_config, holder
            )
            self.active_workloads[task.key] = RunningWorkload(
                future=future,
                command=task.command,
                queue=task.queue,
                executor_config=task.executor_config,
                attempt_number=task.attempt_number,
                holder=holder,
            )

    def _run_workload_in_sandbox(
        self, command: CommandType, executor_config: ExecutorConfigType, holder: SandboxHolder
    ) -> CommandResult:
        """Create a sandbox, run the workload command to completion, then terminate the sandbox."""
        create_kwargs = self._build_create_kwargs(executor_config)
        sandbox = self.client.create(**create_kwargs)
        holder.sandbox = sandbox
        try:
            return sandbox.exec(*command)
        finally:
            with contextlib.suppress(Exception):
                sandbox.terminate()

    def _build_create_kwargs(self, executor_config: ExecutorConfigType) -> dict:
        if executor_config and "command" in executor_config:
            raise ValueError('executor_config must never override the sandbox "command"')
        create_kwargs = deepcopy(self.sandbox_kwargs)
        create_kwargs.update(executor_config or {})
        env = dict(create_kwargs.pop("env", {}) or {})
        env.setdefault("AIRFLOW_IS_EXECUTOR_CONTAINER", "true")
        create_kwargs["env"] = env
        return create_kwargs

    def _collect_finished_workloads(self):
        """Mark finished workloads as success/failure based on the sandbox command result."""
        for key, running in list(self.active_workloads.items()):
            if not running.future.done():
                continue
            del self.active_workloads[key]
            try:
                result = running.future.result()
            except Exception as err:
                self._handle_failed_workload(key, running, str(err))
                continue
            if result.ok:
                self.log.debug("Workload %s succeeded in its sandbox", key)
                self.success(key)
            else:
                self._handle_failed_workload(
                    key, running, result.reason or f"command exited with code {result.errno}"
                )

    def _handle_failed_workload(self, key: WorkloadKey, running: RunningWorkload, reason: str):
        """Reschedule the workload if it has attempts left, otherwise mark it failed."""
        if running.attempt_number < self.max_run_sandbox_attempts:
            self.log.warning(
                "Workload %s failed due to %s. Attempt %s of %s. Rescheduling.",
                key,
                reason,
                running.attempt_number,
                self.max_run_sandbox_attempts,
            )
            self.pending_workloads.append(
                TenkiQueuedTask(
                    key,
                    running.command,
                    running.queue,
                    running.executor_config,
                    running.attempt_number + 1,
                    utcnow() + calculate_next_attempt_delay(running.attempt_number),
                )
            )
        else:
            self.log.error(
                "Workload %s has failed a maximum of %s times. Marking as failed. Reason: %s",
                key,
                running.attempt_number,
                reason,
            )
            self.log_task_event(
                event="tenki sandbox failure",
                ti_key=key,
                extra=f"Sandbox failed after {running.attempt_number} attempts. Reason: {reason}",
            )
            self.fail(key)

    def _report_running_state(self):
        """Emit the sandbox id as the task's external_executor_id once it has been created."""
        for key, running in self.active_workloads.items():
            sandbox = running.holder.sandbox
            if sandbox is not None and not running.holder.id_reported:
                self.running_state(key, sandbox.id)
                running.holder.id_reported = True

    @staticmethod
    def _serialize_workload_to_command(workload) -> CommandType:
        """Serialize a workload into the Task SDK command that runs it."""
        return [
            "python",
            "-m",
            "airflow.sdk.execution_time.execute_workload",
            "--json-string",
            workload.model_dump_json(),
        ]

    def _build_task_command(self, ti: TaskInstance) -> CommandType:
        if AIRFLOW_V_3_0_PLUS:
            from airflow.executors.workloads import ExecuteTask

            workload = ExecuteTask.make(ti)
            return self._serialize_workload_to_command(workload)
        return ti.command_as_list()

    def end(self, heartbeat_interval=10):
        """Wait for all running sandboxes to finish, launching nothing new."""
        try:
            while self.active_workloads:
                self._collect_finished_workloads()
                if self.active_workloads:
                    time.sleep(heartbeat_interval)
        except Exception:
            self.log.exception("Failed to end %s", self.__class__.__name__)
        finally:
            if self._pool is not None:
                self._pool.shutdown(wait=True)

    def terminate(self):
        """Terminate every running sandbox and stop the worker pool."""
        try:
            for running in self.active_workloads.values():
                running.future.cancel()
                sandbox = running.holder.sandbox
                if sandbox is not None:
                    with contextlib.suppress(Exception):
                        sandbox.terminate()
        except Exception:
            self.log.exception("Failed to terminate %s", self.__class__.__name__)
        finally:
            if self._pool is not None:
                self._pool.shutdown(wait=False, cancel_futures=True)

    def revoke_task(self, *, ti: TaskInstance):
        """Remove a task from the executor and terminate its sandbox without changing task state."""
        key = ti.key
        running = self.active_workloads.pop(key, None)
        if running is not None:
            running.future.cancel()
            sandbox = running.holder.sandbox
            if sandbox is not None:
                with contextlib.suppress(Exception):
                    sandbox.terminate()
        self.running.discard(key)
