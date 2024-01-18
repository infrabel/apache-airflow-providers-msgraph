from unittest.mock import patch

from airflow.providers.microsoft.msgraph.hooks.msgraph import MSGraphSDKHook
from assertpy import assert_that
from mockito import mock, when
from msgraph.generated.users.delta.delta_request_builder import DeltaRequestBuilder
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph_beta.generated.sites.item.site_item_request_builder import SiteItemRequestBuilder
from msgraph_beta.generated.sites.sites_request_builder import SitesRequestBuilder
from msgraph_core import APIVersion

from tests.unit.base import BaseTestCase


class MSGraphSDKHookTestCase(BaseTestCase):
    @staticmethod
    async def mock_get(value):
        return value

    def test_get_conn(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            client = self.mock_client()
            hook = MSGraphSDKHook(conn_id="msgraph_api")
            actual = hook.get_conn()

            assert_that(actual).is_same_as(client)

    def test_get_api_version_when_empty_config_dict(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            hook = MSGraphSDKHook(conn_id="msgraph_api")
            actual = hook.get_api_version({})

            assert_that(actual).is_same_as(APIVersion.v1)

    def test_get_api_version_when_api_version_in_config_dict(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            hook = MSGraphSDKHook(conn_id="msgraph_api")
            actual = hook.get_api_version({"api_version": "beta"})

            assert_that(actual).is_same_as(APIVersion.beta)

    def test_run_when_expression_is_valid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            users = self.load_json("resources", "users.json")
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(self.mock_get(users))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            self.mock_client({"users": users_request_builder})

            hook = MSGraphSDKHook(conn_id="msgraph_api")
            actual = self._loop.run_until_complete(hook.evaluate("users.delta.get()"))

            assert_that(actual).is_equal_to(users)

    def test_run_when_expression_with_parameter_is_valid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            site_id = "accinfrabel.sharepoint.com:/sites/news"
            site = self.load_json("resources", "site.json")
            site_item_request_builder = mock(spec=SiteItemRequestBuilder)
            when(site_item_request_builder).get().thenReturn(self.mock_get(site))
            sites_request_builder = mock(spec=SitesRequestBuilder)
            when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
            self.mock_client({"sites": sites_request_builder})

            hook = MSGraphSDKHook(conn_id="msgraph_api")
            actual = self._loop.run_until_complete(hook.evaluate(f"sites.by_site_id('{site_id}').get()"))

            assert_that(actual).is_equal_to(site)

    def test_run_when_expression_with_another_valid_parameter(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            site_id = "accinfrabel.sharepoint.com,dab36736-0b47-44c1-9543-3688bd792230,1b30fecf-4330-4899-b249-104c2afaf9ed"
            site = self.load_json("resources", "site.json")
            site_item_request_builder = mock(spec=SiteItemRequestBuilder)
            when(site_item_request_builder).get().thenReturn(self.mock_get(site))
            sites_request_builder = mock(spec=SitesRequestBuilder)
            when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
            self.mock_client({"sites": sites_request_builder})

            hook = MSGraphSDKHook(conn_id="msgraph_api")
            actual = self._loop.run_until_complete(hook.evaluate(f"sites.by_site_id('{site_id}').get()"))

            assert_that(actual).is_equal_to(site)
