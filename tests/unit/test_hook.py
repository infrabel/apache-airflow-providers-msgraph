from unittest.mock import patch, MagicMock

from airflow.models import Connection
from airflow.providers.microsoft.msgraph.hooks.msgraph import KiotaRequestAdapterHook
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from msgraph_core import APIVersion, NationalClouds

from tests.unit.conftest import get_airflow_connection


class TestGraphServiceClientHook:
    def test_get_conn(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_conn()

            assert isinstance(actual, HttpxRequestAdapter)
            assert actual.base_url == "https://graph.microsoft.com/v1.0"

    def test_api_version(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            assert hook.api_version == APIVersion.v1

    def test_get_api_version_when_empty_config_dict(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_api_version({})

            assert actual == APIVersion.v1

    def test_get_api_version_when_api_version_in_config_dict(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_api_version({"api_version": "beta"})

            assert actual == APIVersion.beta

    def test_get_host_when_connection_has_scheme_and_host(self):
        connection = MagicMock(spec=Connection)
        connection.schema = "https"
        connection.host = "graph.microsoft.de"
        actual = KiotaRequestAdapterHook.get_host(connection)

        assert actual == NationalClouds.Germany.value

    def test_get_host_when_connection_has_no_scheme_or_host(self):
        connection = MagicMock(spec=Connection)
        connection.schema = None
        connection.host = None
        actual = KiotaRequestAdapterHook.get_host(connection)

        assert actual == NationalClouds.Global.value
