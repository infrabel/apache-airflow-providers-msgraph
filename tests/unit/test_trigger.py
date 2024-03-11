import json
import locale
from base64 import b64encode
from unittest.mock import patch

from airflow import AirflowException
from airflow.providers.microsoft.msgraph.triggers.msgraph import MSGraphSDKTrigger
from airflow.triggers.base import TriggerEvent
from assertpy import assert_that
from kiota_http.httpx_request_adapter import HttpxRequestAdapter

from tests.unit.base import Base
from tests.unit.conftest import get_airflow_connection, load_file, mock_json_response, mock_response, load_json


class TestMSGraphSDKTriggerTestCase(Base):
    def test_run_when_valid_response(self):
        users = load_json("resources", "users.json")
        response = mock_json_response(200, users)

        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message", return_value=response),
        ):
            trigger = MSGraphSDKTrigger("users/delta", conn_id="msgraph_api")
            actual = self._loop.run_until_complete(self.run_tigger(trigger))

            assert len(actual) == 1
            assert isinstance(actual[0], TriggerEvent)
            assert actual[0].payload["status"] == "success"
            assert actual[0].payload["type"] == "builtins.dict"
            assert actual[0].payload["response"] == json.dumps(users)

    def test_run_when_response_is_none(self):
        response = mock_json_response(200)

        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message", return_value=response),
        ):
            trigger = MSGraphSDKTrigger("users/delta", conn_id="msgraph_api")
            actual = self._loop.run_until_complete(self.run_tigger(trigger))

            assert len(actual) == 1
            assert isinstance(actual[0], TriggerEvent)
            assert_that(actual[0].payload["status"]).is_equal_to("success")
            assert_that(actual[0].payload["type"]).is_none()
            assert_that(actual[0].payload["response"]).is_none()

    def test_run_when_response_cannot_be_converted_to_json(self):
        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message", side_effect=AirflowException()),
        ):
            trigger = MSGraphSDKTrigger("users/delta", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert isinstance(actual, TriggerEvent)
            assert actual.payload["status"] == "failure"
            assert actual.payload["message"] == ""

    def test_run_when_url_with_response_type_bytes(self):
        content = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
        base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
        response = mock_response(200, content)

        with (
            patch("airflow.hooks.base.BaseHook.get_connection", side_effect=get_airflow_connection),
            patch.object(HttpxRequestAdapter, "get_http_response_message", return_value=response),
        ):
            url = "https://graph.microsoft.com/v1.0/me/drive/items/1b30fecf-4330-4899-b249-104c2afaf9ed/content"
            trigger = MSGraphSDKTrigger(url, response_type="bytes", conn_id="msgraph_api")
            actual = next(iter(self._loop.run_until_complete(self.run_tigger(trigger))))

            assert isinstance(actual, TriggerEvent)
            assert actual.payload["status"] == "success"
            assert actual.payload["type"] == "builtins.bytes"
            assert isinstance(actual.payload["response"], str)
            assert actual.payload["response"] == base64_encoded_content

    def test_serialize(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            url = "https://graph.microsoft.com/v1.0/me/drive/items"
            trigger = MSGraphSDKTrigger(url, response_type="bytes", conn_id="msgraph_api")

            actual = trigger.serialize()

            assert isinstance(actual, tuple)
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
