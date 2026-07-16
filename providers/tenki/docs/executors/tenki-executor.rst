 .. Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

 ..   http://www.apache.org/licenses/LICENSE-2.0

 .. Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

===============
Tenki Executor
===============

The Tenki executor runs each Airflow task inside its own dedicated `Tenki <https://tenki.sh/>`__
sandbox — an ephemeral, network-isolated micro-VM. When the scheduler schedules a task, the
``TenkiExecutor`` serializes the task's workload into a Task SDK command and, in a bounded
worker-thread pool, provisions a sandbox through the ``tenki-sandbox`` SDK and runs that command
inside it via ``Sandbox.exec``. When the command returns, the task is marked ``success`` or
``failed`` based on its exit status and the sandbox is terminated.

Because every task runs in a fresh sandbox, tasks are strongly isolated from each other and from
the scheduler, and each task can request its own image, CPU, memory and environment.

.. note::

    The ``tenki-sandbox`` SDK exposes a blocking ``exec``/``wait`` and a process handle cannot be
    recovered from a sandbox id after reconnecting, so each task's blocking call runs in a worker
    thread whose pool size is bounded by ``[core] parallelism``. As with the ``LocalExecutor``,
    task adoption across scheduler restarts is therefore not supported; orphaned sandboxes are
    bounded by their ``idle_timeout_minutes`` and are terminated when the command returns.

.. contents::
  :local:

How it works
------------

#. The scheduler produces a workload and hands it to the executor.
#. ``TenkiExecutor`` serializes the workload into
   ``python -m airflow.sdk.execution_time.execute_workload --json-string <workload>`` and queues it.
#. On the next heartbeat the executor submits the workload to its worker pool, which calls the
   ``tenki-sandbox`` SDK to create a sandbox and run the command inside it. The sandbox id is
   reported as the task's ``external_executor_id``.
#. When the command returns with exit status ``0`` the task is marked ``success``. On a non-zero
   exit code or a launch error the executor retries the task up to ``max_run_sandbox_attempts``
   times (with exponential backoff) before marking it ``failed``. The sandbox is terminated once
   the command returns.

The sandbox image must contain the same Airflow distribution and Dag code as the scheduler, exactly
like the container image used by the Kubernetes or ECS executors.

Configuration
-------------

Enable the executor by setting it in your ``airflow.cfg`` (or via
``AIRFLOW__CORE__EXECUTOR``)::

    [core]
    executor = airflow.providers.tenki.executors.TenkiExecutor

The executor reads its settings from the ``[tenki_executor]`` section:

.. code-block:: ini

    [tenki_executor]
    # Connection holding the auth token (password), base_url (host) and extra gateway_url/timeout.
    conn_id = tenki_default
    # Or provide them directly (these take precedence over the connection):
    # auth_token = <token>
    # base_url = https://api.tenki.cloud
    # gateway_url = https://gateway.tenki.cloud
    project_id = 0197a921-49f5-72b7-abef-0f54430f49ef   ; required by the service to create sandboxes
    image = myregistry/airflow:3.0.0
    cpu_cores = 2      ; small values (e.g. 1 core / 512 MB) may be terminated on start
    memory_mb = 4096
    idle_timeout_minutes = 30
    max_run_sandbox_attempts = 3
    check_health_on_startup = True

.. note::

    ``project_id`` is required to create sandboxes. Find it in the Tenki console (or from the
    ``who_am_i`` identity). A minimum of roughly ``2`` CPU cores and ``4096`` MB of memory is a
    safe baseline — under-provisioned sandboxes can be terminated immediately on start.

Credentials
-----------

The ``tenki_sandbox.Client`` credentials are resolved in this order:

#. The explicit ``auth_token`` / ``base_url`` / ``gateway_url`` options in ``[tenki_executor]``.
#. The Airflow connection referenced by ``conn_id`` — its ``password`` is the auth token, its
   ``host`` the base URL, and its ``extra`` may hold ``gateway_url`` and ``timeout``.

Store the token in the connection or a secrets backend rather than in plain configuration whenever
possible.

Per-task overrides
------------------

Any keyword argument of ``tenki_sandbox.Client.create()`` can be overridden per task through
``executor_config`` — for example to give a heavier task more resources or a different image:

.. code-block:: python

    from airflow.sdk import task


    @task(executor_config={"cpu_cores": 8, "memory_mb": 16384, "image": "myregistry/airflow-gpu:3.0.0"})
    def heavy_task(): ...

The ``command`` of the sandbox can never be overridden through ``executor_config``.
