import json
from os.path import join, dirname
from typing import Iterable, Dict, Type

import httpx
import msgraph
from airflow.configuration import AirflowConfigParser
from airflow.models import Connection
from airflow.providers.microsoft.msgraph import CLIENT_TYPE
from airflow.providers.microsoft.msgraph.hooks.msgraph import MSGraphSDKHook
from azure import identity
from httpx import Timeout
from kiota_abstractions.authentication import AuthenticationProvider
from kiota_abstractions.request_adapter import RequestAdapter
from kiota_authentication_azure import azure_identity_authentication_provider
from mockito import mock, when
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from msgraph_core import GraphClientFactory, APIVersion

VERSION = "1.0.1"
AirflowConfigParser().load_test_config()
odata_error_type: Type[ODataError] = ODataError  # we need to load this type otherwise tests will fail due to dynamic loading of modules


def load_json(*locations: Iterable[str]):
    with open(join(dirname(__file__), join(*locations)), encoding="utf-8") as file:
        return json.load(file)


def load_file(*locations: Iterable[str]):
    with open(join(dirname(__file__), join(*locations)), encoding="utf-8") as file:
        return file.read()


def mock_httpx_client() -> httpx.AsyncClient:
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


def mock_auth_provider() -> AuthenticationProvider:
    credentials = mock(spec=identity.ClientSecretCredential)
    when(identity).ClientSecretCredential(
        tenant_id="tenant-id",
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


def mock_request_adapter(auth_provider: AuthenticationProvider, httpx_client: httpx.AsyncClient, api_version: APIVersion = APIVersion.v1):
    request_adapter = mock({"base_url": "https://graph.microsoft.com"}, spec=RequestAdapter)
    when(MSGraphSDKHook.sdk_modules[api_version]).GraphRequestAdapter(auth_provider=auth_provider, client=httpx_client).thenReturn(request_adapter)
    return request_adapter


def mock_graph_service_client(request_adapter: msgraph.GraphRequestAdapter, config: Dict = {}, api_version: APIVersion = APIVersion.v1) -> CLIENT_TYPE:
    graph_service_client = mock(config_or_spec={**{"request_adapter": request_adapter}, **config},
                                spec=MSGraphSDKHook.sdk_modules[api_version].GraphServiceClient)
    when(MSGraphSDKHook.sdk_modules[api_version]).GraphServiceClient(request_adapter=request_adapter).thenReturn(graph_service_client)
    return graph_service_client


def mock_client(config: Dict = {}, api_version: APIVersion = APIVersion.v1) -> msgraph.GraphServiceClient:
    httpx_client = mock_httpx_client()
    auth_provider = mock_auth_provider()
    request_adapter = mock_request_adapter(auth_provider=auth_provider, httpx_client=httpx_client, api_version=api_version)
    graph_service_client = mock_graph_service_client(request_adapter=request_adapter, config=config, api_version=api_version)
    return graph_service_client


def get_airflow_connection(conn_id: str, api_version: APIVersion = APIVersion.v1):
    return Connection(
        schema="https",
        conn_id=conn_id,
        conn_type="http",
        host="graph.microsoft.com",
        port="80",
        login="client_id",
        password="client_secret",
        extra={"tenant_id": "tenant-id", "api_version": api_version.value},
    )
