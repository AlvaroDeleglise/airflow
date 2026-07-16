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
"""Data classes, config keys and helpers used by the Tenki executor."""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, TypeAlias

from airflow.providers.tenki.version_compat import AIRFLOW_V_3_3_PLUS

if TYPE_CHECKING:
    from concurrent.futures import Future

    from tenki_sandbox import Sandbox

    if AIRFLOW_V_3_3_PLUS:
        from airflow.executors.workloads.types import WorkloadKey
    else:
        from airflow.models.taskinstance import TaskInstanceKey as WorkloadKey  # type: ignore[assignment]

if AIRFLOW_V_3_3_PLUS:
    from airflow.executors.workloads import ExecuteCallback, ExecuteTask

    CommandType: TypeAlias = Sequence[str] | Sequence[ExecuteTask] | Sequence[ExecuteCallback]
else:
    CommandType: TypeAlias = Sequence[str]  # type: ignore[no-redef, misc]

ExecutorConfigType = dict[str, Any]

CONFIG_GROUP_NAME = "tenki_executor"

CONFIG_DEFAULTS = {
    "conn_id": "tenki_default",
    "max_run_sandbox_attempts": "3",
    "check_health_on_startup": "True",
}

# Keys of the ``[tenki_executor]`` section that are forwarded verbatim to
# ``tenki_sandbox.Client.create()``. Integer-typed keys are coerced before the call.
CREATE_SANDBOX_CONFIG_KEYS = (
    "project_id",
    "workspace_id",
    "image",
    "snapshot_id",
    "cpu_cores",
    "memory_mb",
    "disk_size_gb",
    "idle_timeout_minutes",
)
CREATE_SANDBOX_INT_KEYS = frozenset({"cpu_cores", "memory_mb", "disk_size_gb", "idle_timeout_minutes"})


class TenkiExecutorException(Exception):
    """Raised when something unexpected happens inside the Tenki executor."""


@dataclass
class TenkiQueuedTask:
    """A workload waiting to be launched in a sandbox on the next heartbeat."""

    key: WorkloadKey
    command: CommandType
    queue: str | None
    executor_config: ExecutorConfigType
    attempt_number: int
    next_attempt_time: datetime.datetime


@dataclass
class SandboxHolder:
    """
    Shared handle to the sandbox running a workload.

    The worker thread populates :attr:`sandbox` right after ``create()`` so the main
    thread can read the sandbox id (for ``external_executor_id``) and terminate it on
    ``revoke_task``/``terminate`` without waiting for the blocking ``exec`` to return.
    """

    sandbox: Sandbox | None = None
    id_reported: bool = False


@dataclass
class RunningWorkload:
    """Bookkeeping for a workload currently executing in a sandbox worker thread."""

    future: Future
    command: CommandType
    queue: str | None
    executor_config: ExecutorConfigType
    attempt_number: int
    holder: SandboxHolder = field(default_factory=SandboxHolder)


def calculate_next_attempt_delay(
    attempt_number: int,
    max_delay: int = 60 * 2,
    exponent_base: int = 4,
) -> timedelta:
    """Return the exponential backoff delay until the next launch attempt."""
    return timedelta(seconds=min((exponent_base**attempt_number), max_delay))
