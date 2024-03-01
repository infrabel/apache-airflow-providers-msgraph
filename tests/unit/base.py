import asyncio
from typing import List, Tuple, Any
from unittest import TestCase

from airflow.exceptions import TaskDeferred
from airflow.models import Operator
from airflow.providers.microsoft.msgraph.hooks.msgraph import KiotaRequestAdapterHook
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.state import TaskInstanceState

from tests.unit.conftest import MockedTaskInstance


class BaseTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls._loop = asyncio.get_event_loop()

    def tearDown(self):
        KiotaRequestAdapterHook.cached_request_adapters.clear()
        MockedTaskInstance.values.clear()

    @staticmethod
    async def run_tigger(trigger: BaseTrigger) -> List[TriggerEvent]:
        events = []
        async for event in trigger.run():
            events.append(event)
        return events

    def execute_operator(self, operator: Operator) -> Tuple[Any, Any]:
        task_instance = MockedTaskInstance(task=operator, run_id="run_id", state=TaskInstanceState.RUNNING)
        context = {"ti": task_instance}
        result = None
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
