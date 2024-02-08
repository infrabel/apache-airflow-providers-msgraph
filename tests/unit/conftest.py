import json
from datetime import datetime
from os.path import join, dirname
from typing import Iterable, Dict, Type, Any, Optional, Union

import httpx
import msgraph
from airflow.configuration import AirflowConfigParser
from airflow.models import Connection, TaskInstance
from airflow.providers.microsoft.msgraph.hooks import SDK_MODULES, CLIENT_TYPE
from airflow.utils.session import NEW_SESSION
from airflow.utils.xcom import XCOM_RETURN_KEY
from azure import identity
from httpx import Timeout
from kiota_abstractions.authentication import AuthenticationProvider
from kiota_authentication_azure import azure_identity_authentication_provider
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from mockito import any, mock, when, eq
from msgraph.generated.models.o_data_errors.o_data_error import ODataError
from msgraph_core import GraphClientFactory, APIVersion
from sqlalchemy.orm.session import Session

VERSION = "1.0.2"
AirflowConfigParser().load_test_config()
odata_error_type: Type[ODataError] = ODataError  # we need to load this type otherwise tests will fail due to dynamic loading of modules


def load_json(*locations: Iterable[str]):
    with open(join(dirname(__file__), join(*locations)), encoding="utf-8") as file:
        return json.load(file)


def load_file(*locations: Iterable[str], mode="r", encoding="utf-8"):
    with open(join(dirname(__file__), join(*locations)), mode=mode, encoding=encoding) as file:
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
        api_version=any(APIVersion),
        client=eq(client),
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


def mock_request_adapter(auth_provider: AuthenticationProvider, httpx_client: httpx.AsyncClient, api_version: APIVersion = APIVersion.v1) -> HttpxRequestAdapter:
    request_adapter = mock({"base_url": "https://graph.microsoft.com"}, spec=HttpxRequestAdapter)
    when(SDK_MODULES[api_version]).GraphRequestAdapter(auth_provider=auth_provider, client=httpx_client).thenReturn(request_adapter)
    return request_adapter


def mock_graph_service_client(request_adapter: msgraph.GraphRequestAdapter, config: Dict = {}, api_version: APIVersion = APIVersion.v1) -> CLIENT_TYPE:
    graph_service_client = mock(config_or_spec={**{"request_adapter": request_adapter}, **config},
                                spec=SDK_MODULES[api_version].GraphServiceClient)
    when(SDK_MODULES[api_version]).GraphServiceClient(request_adapter=request_adapter).thenReturn(graph_service_client)
    return graph_service_client


def mock_client(config: Dict = {}, api_version: APIVersion = APIVersion.v1) -> msgraph.GraphServiceClient:
    httpx_client = mock_httpx_client()
    auth_provider = mock_auth_provider()
    request_adapter = mock_request_adapter(auth_provider=auth_provider, httpx_client=httpx_client, api_version=api_version)
    graph_service_client = mock_graph_service_client(request_adapter=request_adapter, config=config, api_version=api_version)
    return graph_service_client


async def return_async(value):
    return value


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


class MockedTaskInstance(TaskInstance):
    values = {}

    def xcom_pull(
        self,
        task_ids: Optional[Union[Iterable[str], str]] = None,
        dag_id: Optional[str] = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
        *,
        map_indexes: Optional[Union[Iterable[int], int]] = None,
        default: Optional[Any] = None,
    ) -> Any:
        self.task_id = task_ids
        self.dag_id = dag_id
        return self.values.get(f"{task_ids}_{dag_id}_{key}")

    def xcom_push(
        self,
        key: str,
        value: Any,
        execution_date: Optional[datetime] = None,
        session: Session = NEW_SESSION,
    ) -> None:
        self.values[f"{self.task_id}_{self.dag_id}_{key}"] = value
