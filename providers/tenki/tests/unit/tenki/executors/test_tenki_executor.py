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
from __future__ import annotations

from concurrent.futures import Future
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.tenki.executors.tenki_executor import TenkiExecutor, TenkiExecutorException
from airflow.utils.state import State

from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test

CONFIG = {
    ("tenki_executor", "auth_token"): "tok",
    ("tenki_executor", "base_url"): "https://api.tenki.sh",
    ("tenki_executor", "project_id"): "proj_123",
    ("tenki_executor", "image"): "myregistry/airflow:3.0.0",
    ("tenki_executor", "cpu_cores"): "2",
    ("tenki_executor", "memory_mb"): "4096",
    ("tenki_executor", "max_run_sandbox_attempts"): "3",
    ("tenki_executor", "check_health_on_startup"): "False",
}


class _SyncPool:
    """A drop-in ThreadPoolExecutor that runs submitted work synchronously for deterministic tests."""

    def submit(self, fn, *args, **kwargs):
        future: Future = Future()
        try:
            future.set_result(fn(*args, **kwargs))
        except Exception as err:
            future.set_exception(err)
        return future

    def shutdown(self, *args, **kwargs):
        pass


def _key(task_id="task"):
    return TaskInstanceKey("dag", task_id, "run", 1, -1)


def _sandbox(sandbox_id="sb-1"):
    sb = mock.MagicMock()
    sb.id = sandbox_id
    return sb


def _result(ok=True, errno=0, reason=None):
    return SimpleNamespace(ok=ok, errno=errno, reason=reason)


@pytest.fixture
def client():
    return mock.MagicMock()


@pytest.fixture
def executor(client):
    with conf_vars(CONFIG):
        ex = TenkiExecutor()
        ex.__dict__["client"] = client
        ex._pool = _SyncPool()
        yield ex


class TestConfig:
    def test_reads_config(self, executor):
        assert executor.max_run_sandbox_attempts == 3
        assert executor.sandbox_kwargs == {
            "project_id": "proj_123",
            "image": "myregistry/airflow:3.0.0",
            "cpu_cores": 2,
            "memory_mb": 4096,
        }

    def test_int_option_must_be_integer(self):
        with conf_vars({**CONFIG, ("tenki_executor", "cpu_cores"): "big"}):
            with pytest.raises(ValueError, match="must be an integer"):
                TenkiExecutor()

    def test_create_sandbox_kwargs_merged(self):
        with conf_vars({**CONFIG, ("tenki_executor", "create_sandbox_kwargs"): '{"tags": ["airflow"]}'}):
            ex = TenkiExecutor()
        assert ex.sandbox_kwargs["tags"] == ["airflow"]


class TestBuildCreateKwargs:
    def test_merges_and_injects_env(self, executor):
        kwargs = executor._build_create_kwargs({"cpu_cores": 8, "env": {"FOO": "bar"}})
        assert kwargs["cpu_cores"] == 8
        assert kwargs["image"] == "myregistry/airflow:3.0.0"
        assert kwargs["project_id"] == "proj_123"
        assert kwargs["env"] == {"FOO": "bar", "AIRFLOW_IS_EXECUTOR_CONTAINER": "true"}

    def test_rejects_command_override(self, executor):
        with pytest.raises(ValueError, match="never override"):
            executor._build_create_kwargs({"command": ["evil"]})


class TestExecuteAsync:
    def test_queues_string_command(self, executor):
        executor.execute_async(_key(), ["echo", "hi"])
        assert len(executor.pending_workloads) == 1
        assert executor.pending_workloads[0].command == ["echo", "hi"]

    def test_rejects_command_in_executor_config(self, executor):
        with pytest.raises(ValueError, match="never override"):
            executor.execute_async(_key(), ["echo"], executor_config={"command": ["x"]})

    def test_serializes_workload(self, executor):
        workload = mock.MagicMock()
        workload.model_dump_json.return_value = '{"a": 1}'
        cmd = executor._serialize_workload_to_command(workload)
        assert cmd == [
            "python",
            "-m",
            "airflow.sdk.execution_time.execute_workload",
            "--json-string",
            '{"a": 1}',
        ]


class TestLaunchAndCollect:
    def test_success(self, executor, client):
        sb = _sandbox("sb-1")
        sb.exec.return_value = _result(ok=True)
        client.create.return_value = sb
        key = _key()

        executor.execute_async(key, ["python", "run"])
        executor._launch_pending_workloads()
        executor._report_running_state()
        assert executor.event_buffer[key][1] == "sb-1"

        executor._collect_finished_workloads()
        assert executor.event_buffer[key][0] == State.SUCCESS
        assert key not in executor.active_workloads
        sb.exec.assert_called_once_with("python", "run")
        sb.terminate.assert_called_once()

        _, kwargs = client.create.call_args
        assert kwargs["image"] == "myregistry/airflow:3.0.0"
        assert kwargs["project_id"] == "proj_123"
        assert kwargs["env"]["AIRFLOW_IS_EXECUTOR_CONTAINER"] == "true"

    def test_nonzero_exit_reschedules(self, executor, client):
        sb = _sandbox()
        sb.exec.return_value = _result(ok=False, errno=1, reason="boom")
        client.create.return_value = sb
        key = _key()

        executor.execute_async(key, ["python", "run"])
        executor._launch_pending_workloads()
        executor._collect_finished_workloads()

        assert len(executor.pending_workloads) == 1
        assert executor.pending_workloads[0].attempt_number == 2
        sb.terminate.assert_called_once()

    def test_failure_at_max_marks_failed(self, executor, client):
        sb = _sandbox()
        sb.exec.return_value = _result(ok=False, errno=1, reason="boom")
        client.create.return_value = sb
        key = _key()

        executor.execute_async(key, ["python", "run"])
        executor.pending_workloads[0].attempt_number = executor.max_run_sandbox_attempts
        executor._launch_pending_workloads()
        executor._collect_finished_workloads()

        assert not executor.pending_workloads
        assert executor.event_buffer[key][0] == State.FAILED

    def test_create_failure_reschedules(self, executor, client):
        client.create.side_effect = RuntimeError("no capacity")
        key = _key()

        executor.execute_async(key, ["python", "run"])
        executor._launch_pending_workloads()
        executor._collect_finished_workloads()

        assert len(executor.pending_workloads) == 1
        assert executor.pending_workloads[0].attempt_number == 2

    def test_backoff_defers_relaunch(self, executor, client):
        sb = _sandbox()
        sb.exec.return_value = _result(ok=False, errno=1, reason="boom")
        client.create.return_value = sb
        key = _key()

        executor.execute_async(key, ["python", "run"])
        executor._launch_pending_workloads()
        executor._collect_finished_workloads()
        # The rescheduled task has a future next_attempt_time, so it is not relaunched yet.
        client.create.reset_mock()
        executor._launch_pending_workloads()
        client.create.assert_not_called()
        assert len(executor.pending_workloads) == 1


class TestLifecycle:
    def test_terminate_stops_sandboxes(self, executor, client):
        sb = _sandbox("sb-1")
        key = _key()
        from airflow.providers.tenki.executors.utils import RunningWorkload, SandboxHolder

        fut: Future = Future()
        fut.set_result(_result())
        executor.active_workloads[key] = RunningWorkload(
            future=fut,
            command=["run"],
            queue=None,
            executor_config={},
            attempt_number=1,
            holder=SandboxHolder(sandbox=sb),
        )
        executor.terminate()
        sb.terminate.assert_called_once()

    def test_revoke_task(self, executor, client):
        sb = _sandbox("sb-1")
        key = _key()
        from airflow.providers.tenki.executors.utils import RunningWorkload, SandboxHolder

        fut: Future = Future()
        fut.set_result(_result())
        executor.active_workloads[key] = RunningWorkload(
            future=fut,
            command=["run"],
            queue=None,
            executor_config={},
            attempt_number=1,
            holder=SandboxHolder(sandbox=sb),
        )
        executor.running.add(key)
        ti = mock.MagicMock()
        ti.key = key
        executor.revoke_task(ti=ti)
        sb.terminate.assert_called_once()
        assert key not in executor.active_workloads
        assert key not in executor.running

    def test_check_health_failure_raises(self, executor, client):
        client.who_am_i.side_effect = RuntimeError("unauthorized")
        with pytest.raises(TenkiExecutorException, match="health check failed"):
            executor.check_health()

    def test_start_skips_health_when_disabled(self, executor, client):
        executor.start()
        client.who_am_i.assert_not_called()
        assert executor._pool is not None
        executor._pool.shutdown(wait=False)
