import asyncio
import json
from os.path import join, dirname
from typing import Iterable, Dict, Type
from unittest import TestCase

import httpx
import msgraph
from airflow.models import Connection
from airflow.providers.microsoft.msgraph import CLIENT_TYPE
from airflow.providers.microsoft.msgraph.hooks.msgraph import MSGraphSDKHook
from azure import identity
from httpx import Timeout
from kiota_abstractions.authentication import AuthenticationProvider
from kiota_abstractions.request_adapter import RequestAdapter
from kiota_authentication_azure import azure_identity_authentication_provider
from mockito import mock, when
from msgraph_core import GraphClientFactory, APIVersion

from msgraph.generated.models.o_data_errors.o_data_error import ODataError


class BaseTestCase(TestCase):
    odata_error_type: Type[ODataError] = ODataError  # we need to load this type otherwise tests will fail due to dynamic loading of modules

    @classmethod
    def setUpClass(cls):
        cls._loop = asyncio.get_event_loop()

    def tearDown(self):
        MSGraphSDKHook.cached_clients.clear()

    @staticmethod
    def load_json(*locations: Iterable[str]):
        with open(join(dirname(__file__), join(*locations)), encoding="utf-8") as file:
            return json.load(file)

    @staticmethod
    def load_file(*locations: Iterable[str]):
        with open(join(dirname(__file__), join(*locations)), encoding="utf-8") as file:
            return file.read()

    @classmethod
    def mock_httpx_client(cls) -> httpx.AsyncClient:
        client = mock(spec=httpx.AsyncClient)
        when(httpx).AsyncClient(
            proxies={},
            timeout=Timeout(timeout=None),
            verify=True,
            trust_env=False,
        ).thenReturn(client)
        httpx_client = mock(spec=httpx.AsyncClient)
        when(GraphClientFactory).create_with_default_middleware(
            api_version=APIVersion.v1.value,
            client=client,
            host="https://graph.microsoft.com",
        ).thenReturn(httpx_client)
        return httpx_client

    @classmethod
    def mock_auth_provider(cls) -> AuthenticationProvider:
        credentials = mock(spec=identity.ClientSecretCredential)
        when(identity).ClientSecretCredential(
            tenant_id="tenant_id",
            client_id="client_id",
            client_secret="client_secret",
            proxies={},
        ).thenReturn(credentials)
        auth_provider = mock(spec=azure_identity_authentication_provider.AzureIdentityAuthenticationProvider)
        when(azure_identity_authentication_provider).AzureIdentityAuthenticationProvider(
            credentials=credentials,
            scopes=["https://graph.microsoft.com/.default"],
        ).thenReturn(auth_provider)
        return auth_provider

    @classmethod
    def mock_request_adapter(cls, auth_provider: AuthenticationProvider, httpx_client: httpx.AsyncClient, api_version: APIVersion = APIVersion.v1):
        request_adapter = mock({"base_url": "https://graph.microsoft.com"}, spec=RequestAdapter)
        when(MSGraphSDKHook.sdk_modules[api_version]).GraphRequestAdapter(auth_provider=auth_provider, client=httpx_client).thenReturn(request_adapter)
        return request_adapter

    @classmethod
    def mock_graph_service_client(cls, request_adapter: msgraph.GraphRequestAdapter, config: Dict = {}, api_version: APIVersion = APIVersion.v1) -> CLIENT_TYPE:
        graph_service_client = mock(config_or_spec={**{"request_adapter": request_adapter}, **config},
                                    spec=MSGraphSDKHook.sdk_modules[api_version].GraphServiceClient)
        when(MSGraphSDKHook.sdk_modules[api_version]).GraphServiceClient(request_adapter=request_adapter).thenReturn(graph_service_client)
        return graph_service_client

    @classmethod
    def mock_client(cls, config: Dict = {}, api_version: APIVersion = APIVersion.v1) -> msgraph.GraphServiceClient:
        httpx_client = cls.mock_httpx_client()
        auth_provider = cls.mock_auth_provider()
        request_adapter = cls.mock_request_adapter(auth_provider=auth_provider, httpx_client=httpx_client, api_version=api_version)
        graph_service_client = cls.mock_graph_service_client(request_adapter=request_adapter, config=config, api_version=api_version)
        return graph_service_client

    @staticmethod
    def get_airflow_connection(conn_id: str, api_version: APIVersion = APIVersion.v1):
        return Connection(
            schema="https",
            conn_id=conn_id,
            conn_type="http",
            host="graph.microsoft.com",
            port="80",
            login="client_id",
            password="client_secret",
            extra={"tenant_id": "tenant_id", "api_version": api_version.value},
        )
