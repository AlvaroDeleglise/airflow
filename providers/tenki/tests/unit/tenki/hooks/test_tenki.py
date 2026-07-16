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

import sys
from types import SimpleNamespace
from unittest import mock

import pytest

from airflow.providers.tenki.hooks.tenki import TenkiSandboxHook


@pytest.fixture
def tenki_sandbox_module():
    """Provide a stub ``tenki_sandbox`` module exposing a mock ``Client``."""
    module = SimpleNamespace(Client=mock.MagicMock(name="Client"))
    with mock.patch.dict(sys.modules, {"tenki_sandbox": module}):
        yield module


def _connection(host="https://conn.tenki.sh", password="conn-token", extra=None):
    conn = mock.MagicMock()
    conn.host = host
    conn.password = password
    conn.extra_dejson = extra or {}
    return conn


class TestTenkiSandboxHook:
    def test_builds_client_from_connection(self, tenki_sandbox_module):
        hook = TenkiSandboxHook(conn_id="my_tenki")
        connection = _connection(extra={"gateway_url": "https://gw", "timeout": 12})
        with mock.patch.object(TenkiSandboxHook, "get_connection", return_value=connection):
            hook.conn
        tenki_sandbox_module.Client.assert_called_once_with(
            auth_token="conn-token",
            base_url="https://conn.tenki.sh",
            gateway_url="https://gw",
            timeout=12.0,
        )

    def test_explicit_values_take_precedence(self, tenki_sandbox_module):
        hook = TenkiSandboxHook(
            conn_id="my_tenki", auth_token="tok", base_url="https://explicit", gateway_url="https://egw"
        )
        with mock.patch.object(TenkiSandboxHook, "get_connection", return_value=_connection()):
            hook.conn
        _, kwargs = tenki_sandbox_module.Client.call_args
        assert kwargs["auth_token"] == "tok"
        assert kwargs["base_url"] == "https://explicit"
        assert kwargs["gateway_url"] == "https://egw"

    def test_no_connection_uses_explicit_only(self, tenki_sandbox_module):
        hook = TenkiSandboxHook(auth_token="tok", base_url="https://explicit")
        with mock.patch.object(TenkiSandboxHook, "get_connection", side_effect=Exception("missing")):
            hook.conn
        tenki_sandbox_module.Client.assert_called_once_with(auth_token="tok", base_url="https://explicit")

    def test_test_connection_success(self, tenki_sandbox_module):
        tenki_sandbox_module.Client.return_value.who_am_i.return_value = "workspace/ws_1"
        hook = TenkiSandboxHook(auth_token="tok", base_url="https://explicit")
        with mock.patch.object(TenkiSandboxHook, "get_connection", side_effect=Exception("missing")):
            ok, message = hook.test_connection()
        assert ok is True
        assert "workspace/ws_1" in message

    def test_test_connection_failure(self, tenki_sandbox_module):
        tenki_sandbox_module.Client.return_value.who_am_i.side_effect = RuntimeError("unauthorized")
        hook = TenkiSandboxHook(auth_token="tok", base_url="https://explicit")
        with mock.patch.object(TenkiSandboxHook, "get_connection", side_effect=Exception("missing")):
            ok, message = hook.test_connection()
        assert ok is False
        assert "unauthorized" in message
