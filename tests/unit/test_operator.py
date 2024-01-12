from typing import List, Any, Tuple
from unittest.mock import patch, MagicMock

import mockito
from airflow import configuration, DAG
from airflow.exceptions import TaskDeferred
from airflow.models import DagModel, Operator, DagBag, DagRun
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator
from airflow.providers.microsoft.msgraph.triggers.msgraph import MSGraphSDKEvaluateTrigger
from airflow.timetables.simple import NullTimetable
from airflow.triggers.base import TriggerEvent
from airflow.utils.state import DagRunState
from assertpy import assert_that
from kiota_serialization_json.json_parse_node import JsonParseNode
from mockito import mock, when, ANY
from msgraph.generated.users.delta.delta_get_response import DeltaGetResponse
from msgraph.generated.users.delta.delta_request_builder import DeltaRequestBuilder
from msgraph.generated.users.users_request_builder import UsersRequestBuilder

from tests.unit.base import BaseTestCase


class MSGraphSDKOperatorTestCase(BaseTestCase):
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

    def execute_operator(self, operator: Operator) -> Tuple[Any,Any]:
        result = None
        triggered_events = []

        with self.assertRaises(TaskDeferred) as deferred:
            operator.execute(context={})

        while deferred.exception:
            events = self._loop.run_until_complete(self.run_tigger(deferred.exception.trigger))

            if not events:
                break

            triggered_events.extend(events)

            try:
                result = operator.execute_complete(context={}, event=next(iter(events)).payload)
                deferred.exception = None
            except TaskDeferred as exception:
                deferred.exception = exception

        return result, triggered_events

    def test_run_when_expression_is_valid_and_keep_events_is_true(self):
        with (patch(
                "airflow.hooks.base.BaseHook.get_connection",
                side_effect=self.get_airflow_connection,
        )):
            users = JsonParseNode(self.load_json("resources", "users.json")).get_object_value(DeltaGetResponse)
            next_users = JsonParseNode(self.load_json("resources", "next_users.json")).get_object_value(
                DeltaGetResponse)
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(self.mock_get(users))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            request_adapter = self.mock_client({"users": users_request_builder}).request_adapter
            when(request_adapter).send_async(
                request_info=ANY,
                parsable_factory=mockito.eq(DeltaGetResponse),
                error_map=ANY,
            ).thenReturn(self.mock_get(next_users))
            operator = MSGraphSDKAsyncOperator(
                task_id="users_delta",
                conn_id="msgraph_api",
                expression="users.delta.get()",
                keep_events=True,
            )

            result, events = self.execute_operator(operator)

            assert_that(result).is_length(2)
            assert_that(events).is_length(2)
            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to(f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[0].payload["response"]).is_equal_to(self.load_json("resources", "users.json"))
            assert_that(events[1]).is_type_of(TriggerEvent)
            assert_that(events[1].payload["status"]).is_equal_to("success")
            assert_that(events[1].payload["type"]).is_equal_to(
                f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[1].payload["response"]).is_equal_to(self.load_json("resources", "next_users.json"))

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

        with patch("airflow.hooks.base.BaseHook.get_connection",side_effect=self.get_airflow_connection), \
             patch.object(DagBag, '__new__', return_value=dag_bag):
            users = JsonParseNode(self.load_json("resources", "users.json")).get_object_value(DeltaGetResponse)
            next_users = JsonParseNode(self.load_json("resources", "next_users.json")).get_object_value(DeltaGetResponse)
            delta_request_builder = mock(spec=DeltaRequestBuilder)
            when(delta_request_builder).get().thenReturn(self.mock_get(users))
            users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
            request_adapter = self.mock_client({"users": users_request_builder}).request_adapter
            when(request_adapter).send_async(
                request_info=ANY,
                parsable_factory=mockito.eq(DeltaGetResponse),
                error_map=ANY,
            ).thenReturn(self.mock_get(next_users))
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

            result, events = self.execute_operator(operator)

            assert_that(result).is_none()
            assert_that(events).is_length(2)
            assert_that(events[0]).is_type_of(TriggerEvent)
            assert_that(events[0].payload["status"]).is_equal_to("success")
            assert_that(events[0].payload["type"]).is_equal_to(f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[0].payload["response"]).is_equal_to(self.load_json("resources", "users.json"))
            assert_that(events[1]).is_type_of(TriggerEvent)
            assert_that(events[1].payload["status"]).is_equal_to("success")
            assert_that(events[1].payload["type"]).is_equal_to(
                f"{DeltaGetResponse.__module__}.{DeltaGetResponse.__name__}")
            assert_that(events[1].payload["response"]).is_equal_to(self.load_json("resources", "next_users.json"))
