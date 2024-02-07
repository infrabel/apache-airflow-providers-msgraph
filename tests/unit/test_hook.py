from unittest.mock import patch

from airflow.providers.microsoft.msgraph.hooks.msgraph import GraphServiceClientHook
from assertpy import assert_that
from msgraph_core import APIVersion

from tests.unit.base import BaseTestCase
from tests.unit.conftest import get_airflow_connection, mock_client


class GraphServiceClientHookTestCase(BaseTestCase):
    def test_get_conn(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            client = mock_client()
            hook = GraphServiceClientHook(conn_id="msgraph_api")
            actual = hook.get_conn()

            assert_that(actual).is_same_as(client)

    def test_api_version(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = GraphServiceClientHook(conn_id="msgraph_api")

            assert_that(hook.api_version).is_same_as(APIVersion.v1)

    def test_get_api_version_when_empty_config_dict(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = GraphServiceClientHook(conn_id="msgraph_api")
            actual = hook.get_api_version({})

            assert_that(actual).is_same_as(APIVersion.v1)

    def test_get_api_version_when_api_version_in_config_dict(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            hook = GraphServiceClientHook(conn_id="msgraph_api")
            actual = hook.get_api_version({"api_version": "beta"})

            assert_that(actual).is_same_as(APIVersion.beta)
