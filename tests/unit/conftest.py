import json
from datetime import datetime
from os.path import join, dirname
from typing import Iterable, Dict, Any, Optional, Union

import httpx
from airflow.configuration import AirflowConfigParser
from airflow.models import TaskInstance
from airflow.utils.session import NEW_SESSION
from airflow.utils.xcom import XCOM_RETURN_KEY
from azure import identity
from httpx import Timeout, Response
from kiota_abstractions.authentication import AuthenticationProvider
from kiota_abstractions.request_information import RequestInformation
from kiota_authentication_azure import azure_identity_authentication_provider
from kiota_http import httpx_request_adapter
from kiota_http.httpx_request_adapter import RESPONSE_HANDLER_EVENT_INVOKED_KEY, HttpxRequestAdapter
from mockito import any, mock, when, eq, ANY
from msgraph_core import GraphClientFactory, APIVersion
from opentelemetry.sdk.trace import Span
from sqlalchemy.orm.session import Session

VERSION = "1.0.2"
AirflowConfigParser().load_test_config()


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


def mock_request_adapter(auth_provider: AuthenticationProvider, httpx_client: httpx.AsyncClient, base_url: str = "https://graph.microsoft.com/v1.0") -> HttpxRequestAdapter:
    request_adapter = mock({"base_url": base_url}, spec=HttpxRequestAdapter)
    when(httpx_request_adapter).HttpxRequestAdapter(authentication_provider=auth_provider, http_client=httpx_client, base_url=base_url).thenReturn(request_adapter)
    return request_adapter


def mock_client() -> HttpxRequestAdapter:
    httpx_client = mock_httpx_client()
    auth_provider = mock_auth_provider()
    request_adapter = mock_request_adapter(auth_provider=auth_provider, httpx_client=httpx_client)
    return request_adapter


def mock_send_primitive_async(request_adapter: HttpxRequestAdapter, *responses: Response, response_type: str = None):
    when(request_adapter).send_primitive_async(
        request_info=any(RequestInformation),
        response_type=eq(response_type),
        error_map=any(dict),
    ).thenCallOriginalImplementation()
    span = mock(spec=Span)
    when(span).end().thenReturn(None)
    when(span).add_event(RESPONSE_HANDLER_EVENT_INVOKED_KEY).thenReturn(None)
    when(request_adapter).start_tracing_span(any(RequestInformation), eq("send_primitive_async")).thenReturn(span)
    when(request_adapter).get_response_handler(any(RequestInformation)).thenCallOriginalImplementation()
    return_values = tuple(map(return_async, responses))
    when(request_adapter).get_http_response_message(any(RequestInformation), ANY).thenReturn(*return_values)
    for response in responses:
        when(request_adapter).throw_failed_responses(eq(response), any(dict), eq(span), eq(span)).thenReturn(
            return_async(None))
        when(request_adapter)._should_return_none(response).thenCallOriginalImplementation()


async def return_async(value):
    return value


def get_airflow_connection(
        conn_id: str,
        login: str = "client_id",
        password: str = "client_secret",
        tenant_id: str = "tenant-id",
        proxies: Optional[Dict] = None,
        api_version: APIVersion = APIVersion.v1):
    from airflow.models import Connection

    return Connection(
        schema="https",
        conn_id=conn_id,
        conn_type="http",
        host="graph.microsoft.com",
        port="80",
        login=login,
        password=password,
        extra={"tenant_id": tenant_id, "api_version": api_version.value, "proxies": proxies or {}},
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
