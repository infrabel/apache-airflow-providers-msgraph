import locale
from base64 import b64encode
from unittest.mock import patch

import msgraph.generated.models.o_data_errors.o_data_error
import msgraph_beta
from airflow.providers.microsoft.msgraph.triggers.msgraph import MSGraphSDKEvaluateTrigger, MSGraphSDKSendAsyncTrigger
from airflow.triggers.base import TriggerEvent
from assertpy import assert_that
from httpx import Response
from kiota_abstractions.request_information import RequestInformation
from kiota_serialization_json.json_parse_node import JsonParseNode
from mockito import mock, when, ANY, eq, any
from msgraph.generated.users.delta.delta_get_response import DeltaGetResponse
from msgraph.generated.users.delta.delta_request_builder import DeltaRequestBuilder
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph_beta.generated.models.site import Site
from msgraph_beta.generated.sites.item.site_item_request_builder import SiteItemRequestBuilder
from msgraph_beta.generated.sites.sites_request_builder import SitesRequestBuilder
from msgraph_core import APIVersion
from opentelemetry.trace import Span

from tests.unit.base import BaseTestCase
from tests.unit.conftest import get_airflow_connection, load_json, mock_client, load_file, return_async


class MSGraphSDKTriggerTestCase(BaseTestCase):
    def test_run_when_expression_is_valid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            users = JsonParseNode(load_json("resources", "users.json")).get_object_value(DeltaGetResponse)
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(return_async(users))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            mock_client({"users": users_request_builder})
            trigger = MSGraphSDKEvaluateTrigger("users.delta.get()", conn_id="msgraph_api")
            actual = self._loop.run_until_complete(self.run_tigger(trigger))

            assert_that(actual).is_length(1)
            assert_that(actual[0]).is_type_of(TriggerEvent)
            assert_that(actual[0].payload["status"]).is_equal_to("success")
            assert_that(actual[0].payload["type"]).is_equal_to(f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(actual[0].payload["response"]).is_equal_to(load_file("resources", "users.json"))

    def test_run_when_expression_is_valid_but_response_is_none(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(return_async(None))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            mock_client({"users": users_request_builder})
            trigger = MSGraphSDKEvaluateTrigger("users.delta.get()", conn_id="msgraph_api")
            actual = self._loop.run_until_complete(self.run_tigger(trigger))

            assert_that(actual).is_length(1)
            assert_that(actual[0]).is_type_of(TriggerEvent)
            assert_that(actual[0].payload["status"]).is_equal_to("success")
            assert_that(actual[0].payload["type"]).is_none()
            assert_that(actual[0].payload["response"]).is_none()

    def test_run_when_expression_is_invalid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            mock_client({"users": users_request_builder})

            trigger = MSGraphSDKEvaluateTrigger("users.delta.get()", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert_that(actual).is_type_of(TriggerEvent)
            assert_that(actual.payload["status"]).is_equal_to("failure")
            assert_that(actual.payload["message"]).is_equal_to("'Dummy' has no attribute 'get' configured")

    def test_run_when_expression_with_parameter_is_valid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            site = JsonParseNode(load_json("resources", "site.json")).get_object_value(Site)
            site_id = "accinfrabel.sharepoint.com:/sites/news"
            site_item_request_builder = mock(spec=SiteItemRequestBuilder)
            when(site_item_request_builder).get().thenReturn(return_async(site))
            sites_request_builder = mock(spec=SitesRequestBuilder)
            when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
            mock_client({"sites": sites_request_builder})

            trigger = MSGraphSDKEvaluateTrigger(f"sites.by_site_id('{site_id}').get()", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert_that(actual).is_type_of(TriggerEvent)
            assert_that(actual.payload["status"]).is_equal_to("success")
            assert_that(actual.payload["type"]).is_equal_to(f"{Site.__module__}.{Site.__name__}")
            assert_that(actual.payload["response"]).is_equal_to(load_file("resources", "site.json"))

    def test_run_when_expression_with_another_valid_parameter(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            site = JsonParseNode(load_json("resources", "site.json")).get_object_value(Site)
            site_id = "accinfrabel.sharepoint.com,dab36736-0b47-44c1-9543-3688bd792230,1b30fecf-4330-4899-b249-104c2afaf9ed"
            site_item_request_builder = mock(spec=SiteItemRequestBuilder)
            when(site_item_request_builder).get().thenReturn(return_async(site))
            sites_request_builder = mock(spec=SitesRequestBuilder)
            when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
            mock_client({"sites": sites_request_builder})

            trigger = MSGraphSDKEvaluateTrigger(f"sites.by_site_id('{site_id}').get()", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert_that(actual).is_type_of(TriggerEvent)
            assert_that(actual.payload["status"]).is_equal_to("success")
            assert_that(actual.payload["type"]).is_equal_to(f"{Site.__module__}.{Site.__name__}")
            assert_that(actual.payload["response"]).is_equal_to(load_file("resources", "site.json"))

    def test_run_when_url_with_response_type_bytes(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            content = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
            base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
            item_id = "1b30fecf-4330-4899-b249-104c2afaf9ed"
            url = f"https://graph.microsoft.com/v1.0/me/drive/items/{item_id}/content"
            request_adapter = mock_client().request_adapter
            response = mock({"status_code": 200, "content": content}, spec=Response)
            span = mock(spec=Span)
            when(span).end().thenReturn(None)
            when(request_adapter).send_primitive_async(
                request_info=any(RequestInformation),
                response_type=eq("bytes"),
                error_map=any(dict),
            ).thenCallOriginalImplementation()
            when(request_adapter).start_tracing_span(any(RequestInformation), eq("send_primitive_async")).thenReturn(span)
            when(request_adapter).get_http_response_message(any(RequestInformation), ANY).thenReturn(return_async(response))
            when(request_adapter).get_response_handler(any(RequestInformation)).thenReturn(None)
            when(request_adapter).throw_failed_responses(eq(response), any(dict), eq(span), eq(span)).thenReturn(return_async(None))
            when(request_adapter)._should_return_none(response).thenCallOriginalImplementation()

            trigger = MSGraphSDKSendAsyncTrigger(url, response_type="bytes", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert_that(actual).is_type_of(TriggerEvent)
            assert_that(actual.payload["status"]).is_equal_to("success")
            assert_that(actual.payload["type"]).is_equal_to(f"{bytes.__module__}.{bytes.__name__}")
            assert_that(actual.payload["response"]).is_type_of(str)
            assert_that(actual.payload["response"]).is_equal_to(base64_encoded_content)

    def test_serialize(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            url = "https://graph.microsoft.com/v1.0/me/drive/items"
            trigger = MSGraphSDKSendAsyncTrigger(url, response_type="bytes", conn_id="msgraph_api")

            actual = trigger.serialize()

            assert_that(actual).is_type_of(tuple)
            assert_that(actual[0]).is_equal_to("airflow.providers.microsoft.msgraph.triggers.msgraph.MSGraphSDKSendAsyncTrigger")
            assert_that(actual[1]).is_equal_to({
                "url": "https://graph.microsoft.com/v1.0/me/drive/items",
                "path_parameters": None,
                "url_template": None,
                "method": 'GET',
                "query_parameters": None,
                "headers": None,
                "content": None,
                "response_type": 'bytes',
                "conn_id": 'msgraph_api',
                "timeout": None,
                "proxies": None,
                "api_version": "v1.0",
                "serializer": "airflow.providers.microsoft.msgraph.serialization.serializer.ResponseSerializer"
            })

    def test_data_error_type(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            trigger = MSGraphSDKSendAsyncTrigger(response_type="bytes", conn_id="msgraph_api")

            actual = trigger.data_error_type

            assert_that(actual).is_equal_to(msgraph.generated.models.o_data_errors.o_data_error.ODataError)

    def test_data_error_type_when_beta(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            trigger = MSGraphSDKSendAsyncTrigger(response_type="bytes", conn_id="msgraph_api", api_version=APIVersion.beta)

            actual = trigger.data_error_type

            assert_that(actual).is_equal_to(msgraph_beta.generated.models.o_data_errors.o_data_error.ODataError)



