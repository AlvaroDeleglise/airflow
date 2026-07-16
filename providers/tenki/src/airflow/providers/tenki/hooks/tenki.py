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
"""Airflow hook that builds a configured Tenki sandbox :class:`~tenki_sandbox.Client`."""

from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Any

from airflow.providers.tenki.version_compat import BaseHook

if TYPE_CHECKING:
    from tenki_sandbox import Client


class TenkiSandboxHook(BaseHook):
    """
    Build an authenticated Tenki sandbox :class:`~tenki_sandbox.Client` from a connection.

    The connection ``password`` holds the API auth token, ``host`` the API base URL and the
    ``extra`` may carry ``gateway_url`` and ``timeout``. Values passed explicitly to the
    constructor take precedence over the connection, which lets the executor source them from
    its own ``[tenki_executor]`` configuration section.

    :param conn_id: Airflow connection id holding the Tenki API credentials.
    :param auth_token: Explicit API token, overriding the connection ``password``.
    :param base_url: Explicit API base URL, overriding the connection ``host``.
    :param gateway_url: Explicit data-plane gateway URL, overriding the connection ``extra``.
    :param timeout: Default RPC timeout in seconds.
    """

    conn_name_attr = "conn_id"
    default_conn_name = "tenki_default"
    conn_type = "tenki"
    hook_name = "Tenki Sandbox"

    def __init__(
        self,
        conn_id: str = default_conn_name,
        auth_token: str | None = None,
        base_url: str | None = None,
        gateway_url: str | None = None,
        timeout: float | None = None,
    ) -> None:
        super().__init__()
        self.conn_id = conn_id
        self._auth_token = auth_token
        self._base_url = base_url
        self._gateway_url = gateway_url
        self._timeout = timeout

    @cached_property
    def conn(self) -> Client:
        """Return the lazily-built Tenki sandbox client."""
        from tenki_sandbox import Client

        auth_token = self._auth_token
        base_url = self._base_url
        gateway_url = self._gateway_url
        timeout = self._timeout

        connection = self._get_connection_safely()
        if connection is not None:
            extra = connection.extra_dejson
            auth_token = auth_token or connection.password
            base_url = base_url or connection.host
            gateway_url = gateway_url or extra.get("gateway_url")
            if timeout is None:
                timeout = extra.get("timeout")

        client_kwargs: dict[str, Any] = {}
        if auth_token:
            client_kwargs["auth_token"] = auth_token
        if base_url:
            client_kwargs["base_url"] = base_url
        if gateway_url:
            client_kwargs["gateway_url"] = gateway_url
        if timeout is not None:
            client_kwargs["timeout"] = float(timeout)
        return Client(**client_kwargs)

    def _get_connection_safely(self):
        try:
            return self.get_connection(self.conn_id)
        except Exception:
            self.log.debug("No Airflow connection %r found, relying on explicit credentials", self.conn_id)
            return None

    def test_connection(self) -> tuple[bool, str]:
        """Validate the connection from the UI by calling ``who_am_i`` on the API."""
        try:
            identity = self.conn.who_am_i()
        except Exception as err:
            return False, str(err)
        return True, f"Successfully authenticated against the Tenki sandbox service as {identity}"
