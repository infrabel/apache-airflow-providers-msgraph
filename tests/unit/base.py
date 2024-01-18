import asyncio
from unittest import TestCase

from airflow.providers.microsoft.msgraph.hooks.msgraph import MSGraphSDKHook


class BaseTestCase(TestCase):
    @classmethod
    def setUpClass(cls):
        cls._loop = asyncio.get_event_loop()

    def tearDown(self):
        MSGraphSDKHook.cached_clients.clear()
