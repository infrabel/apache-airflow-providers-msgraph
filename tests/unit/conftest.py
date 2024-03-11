import json
from datetime import datetime
from os.path import join, dirname
from typing import Iterable, Dict, Any, Optional, Union
from unittest.mock import MagicMock

from airflow.configuration import AirflowConfigParser
from airflow.models import TaskInstance
from airflow.utils.session import NEW_SESSION
from airflow.utils.xcom import XCOM_RETURN_KEY
from httpx import Response
from msgraph_core import APIVersion
from sqlalchemy.orm.session import Session

VERSION = "1.0.2"
AirflowConfigParser().load_test_config()


def load_json(*locations: Iterable[str]):
    with open(join(dirname(__file__), join(*locations)), encoding="utf-8") as file:
        return json.load(file)


def load_file(*locations: Iterable[str], mode="r", encoding="utf-8"):
    with open(join(dirname(__file__), join(*locations)), mode=mode, encoding=encoding) as file:
        return file.read()


def mock_json_response(status_code, *contents) -> Response:
    response = MagicMock(spec=Response)
    response.status_code = status_code
    if contents:
        contents = list(contents)
        response.json.side_effect = lambda: contents.pop(0)
    else:
        response.json.return_value = None
    return response


def mock_response(status_code, content: Any = None) -> Response:
    response = MagicMock(spec=Response)
    response.status_code = status_code
    response.content = content
    return response


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
