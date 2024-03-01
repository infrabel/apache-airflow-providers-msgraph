from unittest.mock import patch

from airflow.models import Connection
from airflow.providers.microsoft.msgraph.hooks.msgraph import KiotaRequestAdapterHook
from assertpy import assert_that
from kiota_http.httpx_request_adapter import HttpxRequestAdapter
from mockito import mock
from msgraph_core import APIVersion, NationalClouds

from tests.unit.base import BaseTestCase
from tests.unit.conftest import get_airflow_connection


class GraphServiceClientHookTestCase(BaseTestCase):
    def test_get_conn(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_conn()

            assert_that(actual).is_type_of(HttpxRequestAdapter)
            assert_that(actual.base_url).is_equal_to("https://graph.microsoft.com/v1.0")

    def test_api_version(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")

            assert_that(hook.api_version).is_same_as(APIVersion.v1)

    def test_get_api_version_when_empty_config_dict(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_api_version({})

            assert_that(actual).is_same_as(APIVersion.v1)

    def test_get_api_version_when_api_version_in_config_dict(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = KiotaRequestAdapterHook(conn_id="msgraph_api")
            actual = hook.get_api_version({"api_version": "beta"})

            assert_that(actual).is_same_as(APIVersion.beta)

    def test_get_host_when_connection_has_scheme_and_host(self):
        connection = mock({"schema": "https", "host": "graph.microsoft.de"}, spec=Connection)
        actual = KiotaRequestAdapterHook.get_host(connection)

        assert_that(actual).is_equal_to(NationalClouds.Germany.value)

    def test_get_host_when_connection_has_no_scheme_or_host(self):
        connection = mock({"schema": None, "host": None}, spec=Connection)
        actual = KiotaRequestAdapterHook.get_host(connection)

        assert_that(actual).is_equal_to(NationalClouds.Global.value)
