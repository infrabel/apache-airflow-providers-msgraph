import json
import locale
from base64 import b64encode
from unittest.mock import patch

from airflow import AirflowException
from airflow.providers.microsoft.msgraph.triggers.msgraph import MSGraphSDKTrigger
from airflow.triggers.base import TriggerEvent
from assertpy import assert_that
from httpx import Response
from mockito import mock, when

from tests.unit.base import BaseTestCase
from tests.unit.conftest import get_airflow_connection, mock_client, load_file, mock_send_primitive_async


class MSGraphSDKTriggerTestCase(BaseTestCase):
    def test_run_when_valid_response(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            users = load_file("resources", "users.json")

            request_adapter = mock_client()
            response = mock({"status_code": 200, "content": users}, spec=Response)
            when(response).json().thenReturn(json.loads(users))
            mock_send_primitive_async(request_adapter, response)

            trigger = MSGraphSDKTrigger("users/delta", conn_id="msgraph_api")
            actual = self._loop.run_until_complete(self.run_tigger(trigger))

            assert_that(actual).is_length(1)
            assert_that(actual[0]).is_type_of(TriggerEvent)
            assert_that(actual[0].payload["status"]).is_equal_to("success")
            assert_that(actual[0].payload["type"]).is_equal_to("builtins.dict")
            assert_that(actual[0].payload["response"]).is_equal_to(users)

    def test_run_when_response_is_none(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            request_adapter = mock_client()
            response = mock({"status_code": 200}, spec=Response)
            when(response).json().thenReturn(None)
            mock_send_primitive_async(request_adapter, response)

            trigger = MSGraphSDKTrigger("users/delta", conn_id="msgraph_api")
            actual = self._loop.run_until_complete(self.run_tigger(trigger))

            assert_that(actual).is_length(1)
            assert_that(actual[0]).is_type_of(TriggerEvent)
            assert_that(actual[0].payload["status"]).is_equal_to("success")
            assert_that(actual[0].payload["type"]).is_none()
            assert_that(actual[0].payload["response"]).is_none()

    def test_run_when_response_cannot_be_converted_to_json(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            request_adapter = mock_client()
            response = mock({"status_code": 200}, spec=Response)
            when(response).json().thenRaise(AirflowException())
            mock_send_primitive_async(request_adapter, response)

            trigger = MSGraphSDKTrigger("users/delta", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert_that(actual).is_type_of(TriggerEvent)
            assert_that(actual.payload["status"]).is_equal_to("failure")
            assert_that(actual.payload["message"]).is_empty()

    def test_run_when_url_with_response_type_bytes(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            content = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
            base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
            item_id = "1b30fecf-4330-4899-b249-104c2afaf9ed"
            url = f"https://graph.microsoft.com/v1.0/me/drive/items/{item_id}/content"
            request_adapter = mock_client()
            response = mock({"status_code": 200, "content": content}, spec=Response)
            mock_send_primitive_async(request_adapter, response, response_type="bytes")

            trigger = MSGraphSDKTrigger(url, response_type="bytes", conn_id="msgraph_api")
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
            trigger = MSGraphSDKTrigger(url, response_type="bytes", conn_id="msgraph_api")

            actual = trigger.serialize()

            assert_that(actual).is_type_of(tuple)
            assert_that(actual[0]).is_equal_to("airflow.providers.microsoft.msgraph.triggers.msgraph.MSGraphSDKTrigger")
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
