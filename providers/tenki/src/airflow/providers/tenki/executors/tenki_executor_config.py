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
Assemble the base ``tenki_sandbox.Client.create()`` keyword arguments from configuration.

These describe the *default* sandbox for every task. They are merged, at launch time, with
the JSON ``create_sandbox_kwargs`` template and then with each task's ``executor_config`` so
individual tasks can request a different image, more CPU, etc.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.tenki.executors.utils import (
    CONFIG_GROUP_NAME,
    CREATE_SANDBOX_CONFIG_KEYS,
    CREATE_SANDBOX_INT_KEYS,
)

if TYPE_CHECKING:
    from airflow.configuration import AirflowConfigParser
    from airflow.providers.tenki.executors.tenki_executor import ConfSource


def build_sandbox_kwargs(conf: AirflowConfigParser | ConfSource) -> dict[str, Any]:
    """Return the base ``Client.create()`` kwargs assembled from the ``[tenki_executor]`` section."""
    base_kwargs: dict[str, Any] = {}
    for key in CREATE_SANDBOX_CONFIG_KEYS:
        value = conf.get(CONFIG_GROUP_NAME, key, fallback=None)
        if value in (None, ""):
            continue
        if key in CREATE_SANDBOX_INT_KEYS:
            try:
                base_kwargs[key] = int(str(value))
            except ValueError:
                raise ValueError(f"[{CONFIG_GROUP_NAME}] {key} must be an integer, got {value!r}")
        else:
            base_kwargs[key] = value

    template = conf.getjson(CONFIG_GROUP_NAME, "create_sandbox_kwargs", fallback={})
    if template:
        if not isinstance(template, dict):
            raise ValueError(
                f"[{CONFIG_GROUP_NAME}] create_sandbox_kwargs must be a JSON object, "
                f"got {type(template).__name__}"
            )
        base_kwargs.update(template)

    return base_kwargs
