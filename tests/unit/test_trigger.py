from typing import List
from unittest.mock import patch

from airflow import configuration
from airflow.providers.microsoft.msgraph.triggers.msgraph import MSGraphSDKEvaluateTrigger
from airflow.triggers.base import TriggerEvent
from assertpy import assert_that
from kiota_serialization_json.json_parse_node import JsonParseNode
from mockito import mock, when
from msgraph.generated.users.delta.delta_get_response import DeltaGetResponse
from msgraph.generated.users.delta.delta_request_builder import DeltaRequestBuilder
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph_beta.generated.models.site import Site
from msgraph_beta.generated.sites.item.site_item_request_builder import SiteItemRequestBuilder
from msgraph_beta.generated.sites.sites_request_builder import SitesRequestBuilder

from tests.unit.base import BaseTestCase


class MSGraphSDKTriggerTestCase(BaseTestCase):
    def setUp(self):
        configuration.load_test_config()

    @staticmethod
    async def mock_get(value):
        return value

    @staticmethod
    async def run_tigger(trigger: MSGraphSDKEvaluateTrigger) -> List[TriggerEvent]:
        events = []
        async for event in trigger.run():
            events.append(event)
        return events

    def test_run_when_expression_is_valid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            users = JsonParseNode(self.load_json("resources", "users.json")).get_object_value(DeltaGetResponse)
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(self.mock_get(users))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            self.mock_client({"users": users_request_builder})
            trigger = MSGraphSDKEvaluateTrigger("users.delta.get()", conn_id="msgraph_api")
            actual = self._loop.run_until_complete(self.run_tigger(trigger))

            assert_that(actual).is_length(1)
            assert_that(actual[0]).is_type_of(TriggerEvent)
            assert_that(actual[0].payload["status"]).is_equal_to("success")
            assert_that(actual[0].payload["type"]).is_equal_to(f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(actual[0].payload["response"]).is_equal_to(self.load_file("resources", "users.json"))

    def test_run_when_expression_is_invalid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            self.mock_client({"users": users_request_builder})

            trigger = MSGraphSDKEvaluateTrigger("users.delta.get()", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert_that(actual).is_type_of(TriggerEvent)
            assert_that(actual.payload["status"]).is_equal_to("failure")
            assert_that(actual.payload["message"]).is_equal_to("'Dummy' has no attribute 'get' configured")

    def test_run_when_expression_with_parameter_is_valid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            site = JsonParseNode(self.load_json("resources", "site.json")).get_object_value(Site)
            site_id = "accinfrabel.sharepoint.com:/sites/news"
            site_item_request_builder = mock(spec=SiteItemRequestBuilder)
            when(site_item_request_builder).get().thenReturn(self.mock_get(site))
            sites_request_builder = mock(spec=SitesRequestBuilder)
            when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
            self.mock_client({"sites": sites_request_builder})

            trigger = MSGraphSDKEvaluateTrigger(f"sites.by_site_id('{site_id}').get()", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert_that(actual).is_type_of(TriggerEvent)
            assert_that(actual.payload["status"]).is_equal_to("success")
            assert_that(actual.payload["type"]).is_equal_to(f"{Site.__module__}.{Site.__name__}")
            assert_that(actual.payload["response"]).is_equal_to(self.load_file("resources", "site.json"))

    def test_run_when_expression_with_another_valid_parameter(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            site = JsonParseNode(self.load_json("resources", "site.json")).get_object_value(Site)
            site_id = "accinfrabel.sharepoint.com,dab36736-0b47-44c1-9543-3688bd792230,1b30fecf-4330-4899-b249-104c2afaf9ed"
            site_item_request_builder = mock(spec=SiteItemRequestBuilder)
            when(site_item_request_builder).get().thenReturn(self.mock_get(site))
            sites_request_builder = mock(spec=SitesRequestBuilder)
            when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
            self.mock_client({"sites": sites_request_builder})

            trigger = MSGraphSDKEvaluateTrigger(f"sites.by_site_id('{site_id}').get()", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert_that(actual).is_type_of(TriggerEvent)
            assert_that(actual.payload["status"]).is_equal_to("success")
            assert_that(actual.payload["type"]).is_equal_to(f"{Site.__module__}.{Site.__name__}")
            assert_that(actual.payload["response"]).is_equal_to(self.load_file("resources", "site.json"))
