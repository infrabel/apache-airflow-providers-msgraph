#
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

import json
from typing import Dict, Optional, Union, TYPE_CHECKING, Tuple
from urllib.parse import urljoin

import httpx
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Connection
from airflow.providers.microsoft.msgraph.hooks import SDK_MODULES, DEFAULT_CONN_NAME
from azure import identity
from httpx import Timeout
from kiota_abstractions.request_adapter import RequestAdapter
from kiota_authentication_azure import azure_identity_authentication_provider
from msgraph_core import GraphClientFactory
from msgraph_core._enums import APIVersion, NationalClouds

if TYPE_CHECKING:
    from airflow.providers.microsoft.msgraph.hooks import CLIENT_TYPE


class GraphServiceClientHook(BaseHook):
    """
    A Microsoft Graph API interaction hook, a Wrapper around Microsoft Graph Client.

    https://github.com/microsoftgraph/msgraph-sdk-python

    :param conn_id: The HTTP Connection ID to run the trigger against.
    :param timeout: The HTTP timeout being used by the msgraph_sdk client (default is None).
        When no timeout is specified or set to None then no HTTP timeout is applied on each request.
    :param proxies: A Dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the msgraph_sdk client to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as "v1.0" or "beta".
        This will determine which msgraph_sdk client is going to be used as each version has a dedicated client.
    """

    cached_clients: Dict[str, Tuple[APIVersion, CLIENT_TYPE]] = {}

    def __init__(
        self,
        conn_id: str = DEFAULT_CONN_NAME,
        timeout: Optional[float] = None,
        proxies: Optional[Dict] = None,
        api_version: Optional[Union[APIVersion, str]] = None,
    ) -> None:
        self.conn_id = conn_id
        self.timeout = timeout
        self.proxies = proxies
        self._api_version = self.resolve_api_version_from_value(api_version)

    @property
    def request_adapter(self) -> RequestAdapter:
        return self.get_conn().request_adapter

    @property
    def api_version(self) -> APIVersion:
        self.get_conn()  # Make sure config has been loaded through get_conn to have correct api version!
        return self._api_version

    @staticmethod
    def resolve_api_version_from_value(
        api_version: Union[APIVersion, str], default: Optional[APIVersion] = None
    ) -> APIVersion:
        if isinstance(api_version, APIVersion):
            return api_version
        return next(
            filter(lambda version: version.value == api_version, APIVersion),
            default,
        )

    def get_api_version(self, config: Dict) -> APIVersion:
        if self._api_version is None:
            return self.resolve_api_version_from_value(
                api_version=config.get("api_version"), default=APIVersion.v1
            )
        return self._api_version

    @staticmethod
    def get_host(connection: Connection) -> str:
        if connection.schema and connection.host:
            return f"{connection.schema}://{connection.host}"
        return NationalClouds.Global.value

    @staticmethod
    def to_httpx_proxies(proxies: Dict) -> Dict:
        proxies = proxies.copy()
        if proxies.get("http"):
            proxies["http://"] = proxies.pop("http")
        if proxies.get("https"):
            proxies["https://"] = proxies.pop("https")
        return proxies

    def get_conn(self) -> CLIENT_TYPE:
        if not self.conn_id:
            raise AirflowException(
                "Failed to create Microsoft Graph SDK client. No conn_id provided!"
            )

        api_version, client = self.cached_clients.get(self.conn_id, (None, None))

        if not client:
            connection = self.get_connection(conn_id=self.conn_id)
            client_id = connection.login
            client_secret = connection.password
            config = connection.extra_dejson if connection.extra else {}
            tenant_id = config.get("tenant_id")
            api_version = self.get_api_version(config)
            host = self.get_host(connection)
            base_url = config.get("base_url", urljoin(host, api_version.value))
            proxies = self.proxies or config.get("proxies", {})
            scopes = config.get("scopes", ["https://graph.microsoft.com/.default"])
            verify = config.get("verify", True)
            trust_env = config.get("trust_env", False)

            self.log.info(
                "Creating Microsoft Graph SDK client %s for conn_id: %s",
                api_version.value,
                self.conn_id,
            )
            self.log.info("Host: %s", host)
            self.log.info("Base URL: %s", base_url)
            self.log.info("Tenant id: %s", tenant_id)
            self.log.info("Client id: %s", client_id)
            self.log.info("Client secret: %s", client_secret)
            self.log.info("API version: %s", api_version.value)
            self.log.info("Scope: %s", scopes)
            self.log.info("Verify: %s", verify)
            self.log.info("Timeout: %s", self.timeout)
            self.log.info("Trust env: %s", trust_env)
            self.log.info("Proxies: %s", json.dumps(proxies))
            credentials = identity.ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=connection.login,
                client_secret=connection.password,
                proxies=proxies,
            )
            http_client = GraphClientFactory.create_with_default_middleware(
                api_version=api_version,
                client=httpx.AsyncClient(
                    proxies=self.to_httpx_proxies(proxies),
                    timeout=Timeout(timeout=self.timeout),
                    verify=verify,
                    trust_env=trust_env,
                ),
                host=host,
            )
            auth_provider = azure_identity_authentication_provider.AzureIdentityAuthenticationProvider(
                credentials=credentials, scopes=scopes
            )
            request_adapter = SDK_MODULES[api_version].GraphRequestAdapter(
                auth_provider=auth_provider, client=http_client
            )
            request_adapter.base_url = base_url
            client = SDK_MODULES[api_version].GraphServiceClient(
                request_adapter=request_adapter
            )
            self.cached_clients[self.conn_id] = (api_version, client)
        self._api_version = api_version
        return client
