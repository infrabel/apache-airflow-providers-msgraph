from typing import List, Any, Tuple
from unittest.mock import patch, MagicMock

import mockito
from airflow import DAG
from airflow.exceptions import TaskDeferred, AirflowException
from airflow.models import DagModel, Operator, DagBag, DagRun
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator
from airflow.providers.microsoft.msgraph.triggers.msgraph import MSGraphSDKEvaluateTrigger
from airflow.timetables.simple import NullTimetable
from airflow.triggers.base import TriggerEvent
from airflow.utils.state import DagRunState, TaskInstanceState
from assertpy import assert_that
from kiota_serialization_json.json_parse_node import JsonParseNode
from mockito import mock, when, ANY
from msgraph.generated.users.delta.delta_get_response import DeltaGetResponse
from msgraph.generated.users.delta.delta_request_builder import DeltaRequestBuilder
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph_beta.generated.drives.drives_request_builder import DrivesRequestBuilder
from msgraph_beta.generated.drives.item.drive_item_request_builder import DriveItemRequestBuilder
from msgraph_beta.generated.drives.item.items.item.drive_item_item_request_builder import DriveItemItemRequestBuilder
from msgraph_beta.generated.drives.item.items.items_request_builder import ItemsRequestBuilder
from msgraph_beta.generated.drives.item.root.content.content_request_builder import ContentRequestBuilder

from tests.unit.base import BaseTestCase
from tests.unit.conftest import load_json, mock_client, get_airflow_connection, load_file, MockedTaskInstance, \
    return_async


class MSGraphSDKOperatorTestCase(BaseTestCase):
    @staticmethod
    async def run_tigger(trigger: MSGraphSDKEvaluateTrigger) -> List[TriggerEvent]:
        events = []
        async for event in trigger.run():
            events.append(event)
        return events

    def execute_operator(self, operator: Operator) -> Tuple[Any,Any]:
        task_instance = MockedTaskInstance(task=operator, run_id="run_id", state=TaskInstanceState.RUNNING)
        context = {"ti": task_instance}
        triggered_events = []

        with self.assertRaises(TaskDeferred) as deferred:
            operator.execute(context=context)

        while deferred.exception:
            events = self._loop.run_until_complete(self.run_tigger(deferred.exception.trigger))

            if not events:
                break

            triggered_events.extend(events)

            try:
                method = getattr(operator, deferred.exception.method_name)
                result = method(context=context, event=next(iter(events)).payload)
                deferred.exception = None
            except TaskDeferred as exception:
                deferred.exception = exception

        return result, triggered_events

    def test_run_when_expression_is_valid(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            users = load_json("resources", "users.json")
            next_users = load_json("resources", "next_users.json")
            response = JsonParseNode(users).get_object_value(DeltaGetResponse)
            next_response = JsonParseNode(next_users).get_object_value(
                DeltaGetResponse)
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(return_async(response))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            request_adapter = mock_client({"users": users_request_builder}).request_adapter
            when(request_adapter).send_async(
                request_info=ANY,
                parsable_factory=mockito.eq(DeltaGetResponse),
                error_map=ANY,
            ).thenReturn(return_async(next_response))
            operator = MSGraphSDKAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                expression="users.delta.get()",
            )

            results, events = self.execute_operator(operator)

            assert_that(results).is_length(2)
            assert_that(results[0]).is_type_of(dict)
            assert_that(results[0]).contains_entry({"@odata.context":
                                                    "https://graph.microsoft.com/v1.0/$metadata#users(displayName,description,mailNickname)"})
            assert_that(results[1]).is_type_of(dict)
            assert_that(results[1]).contains_entry({"@odata.context":
                                                    "https://graph.microsoft.com/v1.0/$metadata#users(displayName,description,mailNickname)"})
            assert_that(events).is_length(2)
            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to(f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[0].payload["response"]).is_equal_to(users)
            assert_that(events[1]).is_type_of(TriggerEvent)
            assert_that(events[1].payload["status"]).is_equal_to("success")
            assert_that(events[1].payload["type"]).is_equal_to(
                f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[1].payload["response"]).is_equal_to(next_users)

    def test_run_when_expression_is_valid_and_do_xcom_push_is_false(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            users = load_json("resources", "users.json")
            users.pop("@odata.nextLink")
            response = JsonParseNode(users).get_object_value(DeltaGetResponse)
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(return_async(response))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            mock_client({"users": users_request_builder})
            operator = MSGraphSDKAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                expression="users.delta.get()",
                do_xcom_push=False,
            )

            results, events = self.execute_operator(operator)

            assert_that(results).is_type_of(dict)
            assert_that(events).is_length(1)
            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to(
                f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[0].payload["response"]).is_equal_to(users)

    def test_run_when_an_exception_occurs(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenRaise(AirflowException("The conn_id `msgraph_api` isn't defined"))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            mock_client({"users": users_request_builder})
            operator = MSGraphSDKAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                expression="users.delta.get()",
                do_xcom_push=False,
            )

            with self.assertRaises(AirflowException):
                self.execute_operator(operator)

    def test_run_when_valid_expression_and_trigger_dag_id(self):
        trigger_dag_id = "triggered_dag_id"

        dag_run = mock(spec=DagRun)
        dag = mock({"default_args": [], "timetable": NullTimetable(), "subdags": []}, spec=DAG)
        when(dag).create_dagrun(
            run_id=ANY,
            execution_date=ANY,
            state=mockito.eq(DagRunState.QUEUED),
            conf=ANY,
            external_trigger=True,
            dag_hash=mockito.eq(trigger_dag_id),
            data_interval=ANY,
        ).thenReturn(dag_run)

        dag_bag = MagicMock()
        dag_bag.dags = {trigger_dag_id}
        dag_bag.dags_hash = {trigger_dag_id: trigger_dag_id}
        dag_bag.get_dag.side_effect=lambda dag_id: dag

        with patch("airflow.hooks.base.BaseHook.get_connection",side_effect=get_airflow_connection), \
            patch.object(DagBag, '__new__', return_value=dag_bag):
            users = load_json("resources", "users.json")
            next_users = load_json("resources", "next_users.json")
            response = JsonParseNode(users).get_object_value(DeltaGetResponse)
            next_response = JsonParseNode(next_users).get_object_value(DeltaGetResponse)
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(return_async(response))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            request_adapter = mock_client({"users": users_request_builder}).request_adapter
            when(request_adapter).send_async(
                request_info=ANY,
                parsable_factory=mockito.eq(DeltaGetResponse),
                error_map=ANY,
            ).thenReturn(return_async(next_response))
            dag_model = mock(spec=DagModel)
            mockito.patch(dag_model, "fileloc", __file__)
            when(DagModel).get_current(trigger_dag_id).thenReturn(dag_model)
            when(DagRun).find_duplicate(dag_id=mockito.eq(trigger_dag_id), execution_date=ANY, run_id=ANY).thenReturn(None)
            operator = MSGraphSDKAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                expression="users.delta.get()",
                trigger_dag_id=trigger_dag_id,
            )

            results, events = self.execute_operator(operator)

            assert_that(results).is_none()
            assert_that(events).is_length(2)

            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to(f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[0].payload["response"]).is_equal_to(users)
            assert_that(events[1]).is_type_of(TriggerEvent)
            assert_that(events[1].payload["status"]).is_equal_to("success")
            assert_that(events[1].payload["type"]).is_equal_to(
                f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[1].payload["response"]).is_equal_to(next_users)

    def test_run_when_valid_expression_which_returns_bytes(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=get_airflow_connection,
        )):
            content = load_file("resources", "users.json").encode()
            drive_id = "82f9d24d-6891-4790-8b6d-f1b2a1d0ca22"
            drive_item_id = "100"
            content_request_builder = mock(spec=ContentRequestBuilder)
            when(content_request_builder).get().thenReturn(return_async(content))
            drive_item_item_request_builder = mock({"content": content_request_builder}, spec=DriveItemItemRequestBuilder)
            items_request_builder = mock( spec=ItemsRequestBuilder)
            when(items_request_builder).by_drive_item_id(drive_item_id).thenReturn(drive_item_item_request_builder)
            drive_item_request_builder = mock({"items": items_request_builder}, spec=DriveItemRequestBuilder)

            drives_request_builder = mock(spec=DrivesRequestBuilder)
            when(drives_request_builder).by_drive_id(drive_id).thenReturn(drive_item_request_builder)
            mock_client({"drives": drives_request_builder})
            operator = MSGraphSDKAsyncOperator(
                task_id="drive_item_content",
                conn_id="msgraph_api",
                expression=f"drives.by_drive_id('{drive_id}').items.by_drive_item_id('{drive_item_id}').content.get()",
            )

            results, events = self.execute_operator(operator)

            assert_that(results).is_equal_to(content)
            assert_that(events).is_length(1)
            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to(f"{bytes.__module__}.{bytes.__name__}")
            assert_that(events[0].payload["response"]).is_equal_to(content)
