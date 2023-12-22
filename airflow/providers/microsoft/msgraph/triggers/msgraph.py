#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import annotations

from abc import abstractmethod
from typing import Dict, Optional, Any, AsyncIterator, Sequence, Union

from airflow.providers.microsoft.msgraph import DEFAULT_CONN_NAME
from airflow.providers.microsoft.msgraph.hooks.msgraph import MSGraphSDKHook
from airflow.providers.microsoft.msgraph.triggers.serializer import ResponseSerializer
from airflow.triggers.base import BaseTrigger, TriggerEvent
from msgraph_core import APIVersion


class MSGraphSDKBaseTrigger(BaseTrigger):
    def __init__(
        self,
        conn_id: str = DEFAULT_CONN_NAME,
        timeout: Optional[float] = None,
        proxies: Optional[Dict] = None,
        api_version: Union[APIVersion, str] = APIVersion.v1,
    ):
        super().__init__()
        self.conn_id = conn_id
        self.timeout = timeout
        self.proxies = proxies
        self.api_version = api_version

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes HttpTrigger arguments and classpath."""
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "timeout": self.timeout,
                "proxies": self.proxies,
                "api_version": self.api_version.value,
            },
        )

    @property
    def hook(self) -> MSGraphSDKHook:
        return MSGraphSDKHook(
            conn_id=self.conn_id,
            timeout=self.timeout,
            proxies=self.proxies,
            api_version=self.api_version,
        )

    @abstractmethod
    async def execute(self) -> Any:
        raise NotImplementedError()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Makes a series of asynchronous http calls via a MSGraphSDKHook."""
        try:
            response = await self.execute()

            self.log.info("response: %s", response)

            if response:
                response_type = type(response)
                yield TriggerEvent(
                    {
                        "status": "success",
                        "type": f"{response_type.__module__}.{response_type.__name__}",
                        "response": ResponseSerializer.serialize(response),
                    }
                )
        except Exception as e:
            yield TriggerEvent({"status": "failure", "message": str(e)})


class MSGraphSDKEvaluateTrigger(MSGraphSDKBaseTrigger):
    """
    A Microsoft Graph API trigger which allows you to execute an expression on the msgraph_sdk client.

    https://github.com/microsoftgraph/msgraph-sdk-python

    :param expression: The expression being executed on the msgraph_sdk client (templated).
    :param conn_id: The HTTP Connection ID to run the trigger against (templated).
    :param timeout: The HTTP timeout being used by the msgraph_sdk client (default is None).
        When no timeout is specified or set to None then no HTTP timeout is applied on each request.
    :param proxies: A Dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the msgraph_sdk client to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as "v1.0" or "beta".
        This will determine which msgraph_sdk client is going to be used as each version has a dedicated client.
    """

    template_fields: Sequence[str] = ("expression", "conn_id")

    def __init__(
        self,
        expression: Optional[str] = None,
        conn_id: str = DEFAULT_CONN_NAME,
        timeout: Optional[float] = None,
        proxies: Optional[Dict] = None,
        api_version: Union[APIVersion, str] = APIVersion.v1,
    ):
        super().__init__(
            conn_id=conn_id, timeout=timeout, proxies=proxies, api_version=api_version
        )
        self.expression = expression

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes MSGraphSDKEvaluateTrigger arguments and classpath."""
        name, fields = super().serialize()
        fields = {**{"expression": self.expression}, **fields}
        return name, fields

    async def execute(self) -> AsyncIterator[TriggerEvent]:
        return await self.hook.evaluate(
            expression=self.expression,
        )


class MSGraphSDKAsyncSendTrigger(MSGraphSDKBaseTrigger):
    """
    A Microsoft Graph API trigger which allows you to executean async URL request using the msgraph_sdk client.

    https://github.com/microsoftgraph/msgraph-sdk-python

    :param url: The url being executed on the msgraph_sdk client (templated).
    :param conn_id: The HTTP Connection ID to run the trigger against (templated).
    :param timeout: The HTTP timeout being used by the msgraph_sdk client (default is None).
        When no timeout is specified or set to None then no HTTP timeout is applied on each request.
    :param proxies: A Dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the msgraph_sdk client to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as "v1.0" or "beta".
        This will determine which msgraph_sdk client is going to be used as each version has a dedicated client.
    """

    template_fields: Sequence[str] = ("url", "conn_id")

    def __init__(
        self,
        url: Optional[str] = None,
        response_type: Optional[str] = None,
        conn_id: str = DEFAULT_CONN_NAME,
        timeout: Optional[float] = None,
        proxies: Optional[Dict] = None,
        api_version: Union[APIVersion, str] = APIVersion.v1,
    ):
        super().__init__(
            conn_id=conn_id, timeout=timeout, proxies=proxies, api_version=api_version
        )
        self.url = url
        self.response_type = response_type

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes MSGraphSDKAsyncSendTrigger arguments and classpath."""
        name, fields = super().serialize()
        fields = {**{"url": self.url, "response_type": self.response_type}, **fields}
        return name, fields

    async def execute(self) -> AsyncIterator[TriggerEvent]:
        return await self.hook.send_async(
            url=self.url, response_type=self.response_type
        )
