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
Example Dag showing how tasks run under the Tenki sandbox executor.

Nothing here is Tenki-specific except the optional per-task ``executor_config``: when the
deployment's ``[core] executor`` is the ``TenkiExecutor``, every task instance is executed
inside its own ephemeral Tenki sandbox. A task can request more resources or a different
image for its sandbox via ``executor_config`` — those keys are forwarded to
``tenki_sandbox.Client.create()``.
"""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG, task

with DAG(
    dag_id="example_tenki",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example", "tenki"],
) as dag:

    @task
    def extract() -> list[int]:
        return [1, 2, 3]

    @task
    def total(values: list[int]) -> int:
        return sum(values)

    # This task asks the Tenki executor for a bigger sandbox and a different image.
    # The executor_config keys are passed straight to tenki_sandbox.Client.create().
    @task(
        executor_config={
            "cpu_cores": 4,
            "memory_mb": 8192,
            "image": "myregistry/airflow-heavy:latest",
        }
    )
    def heavy_step(value: int) -> str:
        return f"processed total={value} in a dedicated sandbox"

    heavy_step(total(extract()))
