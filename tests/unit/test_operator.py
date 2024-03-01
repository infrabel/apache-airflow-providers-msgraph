import locale
from base64 import b64encode
from unittest.mock import patch

from airflow.exceptions import AirflowException
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator
from airflow.triggers.base import TriggerEvent
from assertpy import assert_that
from httpx import Response
from mockito import mock, when

from tests.unit.base import BaseTestCase
from tests.unit.conftest import load_json, mock_client, get_airflow_connection, load_file, mock_send_primitive_async


class MSGraphSDKOperatorTestCase(BaseTestCase):
    def test_run_when_expression_is_valid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            users = load_json("resources", "users.json")
            next_users = load_json("resources", "next_users.json")
            request_adapter = mock_client()
            response = mock({"status_code": 200}, spec=Response)
            when(response).json().thenReturn(users)
            next_response = mock({"status_code": 200}, spec=Response)
            when(next_response).json().thenReturn(next_users)
            mock_send_primitive_async(request_adapter, response, next_response)
            operator = MSGraphSDKAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users",
                result_processor=lambda context, result: result.get("value")
            )

            results, events = self.execute_operator(operator)

            assert_that(results).is_length(30)
            assert_that(results).is_equal_to(users.get("value") + next_users.get("value"))
            assert_that(events).is_length(2)
            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to("builtins.dict")
            assert_that(events[0].payload["response"]).is_equal_to(users.get("value"))
            assert_that(events[1]).is_type_of(TriggerEvent)
            assert_that(events[1].payload["status"]).is_equal_to("success")
            assert_that(events[1].payload["type"]).is_equal_to("builtins.dict")
            assert_that(events[1].payload["response"]).is_equal_to(next_users.get("value"))

    def test_run_when_expression_is_valid_and_do_xcom_push_is_false(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            users = load_json("resources", "users.json")
            users.pop("@odata.nextLink")
            request_adapter = mock_client()
            response = mock({"status_code": 200}, spec=Response)
            when(response).json().thenReturn(users)
            mock_send_primitive_async(request_adapter, response)
            operator = MSGraphSDKAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users/delta",
                do_xcom_push=False,
            )

            results, events = self.execute_operator(operator)

            assert_that(results).is_type_of(dict)
            assert_that(events).is_length(1)
            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to("builtins.dict")
            assert_that(events[0].payload["response"]).is_equal_to(users)

    def test_run_when_an_exception_occurs(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            request_adapter = mock_client()
            response = mock({"status_code": 500}, spec=Response)
            when(response).json().thenRaise(AirflowException())
            mock_send_primitive_async(request_adapter, response)
            operator = MSGraphSDKAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                url="users/delta",
                do_xcom_push=False,
            )

            with self.assertRaises(AirflowException):
                self.execute_operator(operator)

    def test_run_when_url_which_returns_bytes(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            content = load_file("resources", "dummy.pdf", mode="rb", encoding=None)
            base64_encoded_content = b64encode(content).decode(locale.getpreferredencoding())
            drive_id = "82f9d24d-6891-4790-8b6d-f1b2a1d0ca22"
            request_adapter = mock_client()
            response = mock({"status_code": 500, "content": content}, spec=Response)
            mock_send_primitive_async(request_adapter, response, response_type="bytes")
            operator = MSGraphSDKAsyncOperator(
                task_id="drive_item_content",
                conn_id="msgraph_api",
                response_type="bytes",
                url=f"/drives/{drive_id}/root/content",
            )

            results, events = self.execute_operator(operator)

            assert_that(results).is_equal_to(base64_encoded_content)
            assert_that(events).is_length(1)
            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to(f"{bytes.__module__}.{bytes.__name__}")
            assert_that(events[0].payload["response"]).is_equal_to(base64_encoded_content)
