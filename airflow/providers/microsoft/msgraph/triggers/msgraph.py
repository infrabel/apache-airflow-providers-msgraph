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
from contextlib import suppress
from functools import cached_property
from io import BytesIO
from typing import Dict, Optional, Any, AsyncIterator, Sequence, Union, Type

from airflow.providers.microsoft.msgraph.hooks import (
    DEFAULT_CONN_NAME,
    CLIENT_TYPE,
    ODATA_ERROR_TYPE,
)
from airflow.providers.microsoft.msgraph.hooks.evaluator import ExpressionEvaluator
from airflow.providers.microsoft.msgraph.hooks.msgraph import GraphServiceClientHook
from airflow.providers.microsoft.msgraph.serialization.serializer import (
    ResponseSerializer,
)
from airflow.triggers.base import BaseTrigger, TriggerEvent
from airflow.utils.module_loading import import_string
from kiota_abstractions.method import Method
from kiota_abstractions.request_adapter import ResponseType, RequestAdapter
from kiota_abstractions.request_information import RequestInformation, QueryParams
from kiota_abstractions.serialization import ParsableFactory, Parsable
from msgraph_core import APIVersion


class MSGraphSDKBaseTrigger(BaseTrigger):
    def __init__(
        self,
        conn_id: str = DEFAULT_CONN_NAME,
        timeout: Optional[float] = None,
        proxies: Optional[Dict] = None,
        api_version: Optional[APIVersion] = None,
        serializer: Union[str, Type[ResponseSerializer]] = ResponseSerializer,
    ):
        super().__init__()
        self.hook = GraphServiceClientHook(
            conn_id=conn_id,
            timeout=timeout,
            proxies=proxies,
            api_version=api_version,
        )
        self.serializer: ResponseSerializer = self.resolve_serializer(serializer)

    @classmethod
    def resolve_serializer(
        cls, serializer: Union[str, Type[ResponseSerializer]]
    ) -> ResponseSerializer:
        if isinstance(serializer, str):
            try:
                serializer = import_string(serializer)
            except ImportError:
                serializer = ResponseSerializer
        return serializer()

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes HttpTrigger arguments and classpath."""
        api_version = self.api_version.value if self.api_version else None
        return (
            f"{self.__class__.__module__}.{self.__class__.__name__}",
            {
                "conn_id": self.conn_id,
                "timeout": self.timeout,
                "proxies": self.proxies,
                "api_version": api_version,
                "serializer": f"{self.serializer.__class__.__module__}.{self.serializer.__class__.__name__}",
            },
        )

    def get_conn(self) -> CLIENT_TYPE:
        return self.hook.get_conn()

    @property
    def conn_id(self) -> str:
        return self.hook.conn_id

    @property
    def timeout(self) -> Optional[float]:
        return self.hook.timeout

    @property
    def proxies(self) -> Optional[Dict]:
        return self.hook.proxies

    @property
    def api_version(self) -> APIVersion:
        return self.hook.api_version

    @property
    def request_adapter(self) -> RequestAdapter:
        return self.get_conn().request_adapter

    @abstractmethod
    async def execute(self) -> Any:
        raise NotImplementedError()

    async def run(self) -> AsyncIterator[TriggerEvent]:
        """Makes a series of asynchronous http calls via a MSGraphSDKHook."""
        try:
            response = await self.execute()

            self.log.debug("response: %s", response)

            if response:
                response_type = type(response)

                self.log.debug("response type: %s", type(response))

                response = self.serializer.serialize(response)

                self.log.debug("serialized response type: %s", type(response))

                yield TriggerEvent(
                    {
                        "status": "success",
                        "type": f"{response_type.__module__}.{response_type.__name__}",
                        "response": response,
                    }
                )
            else:
                yield TriggerEvent(
                    {
                        "status": "success",
                        "type": None,
                        "response": None,
                    }
                )
        except Exception as e:
            self.log.exception("An error occurred: %s", e)
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
        serializer: Union[str, Type[ResponseSerializer]] = ResponseSerializer,
    ):
        super().__init__(
            conn_id=conn_id,
            timeout=timeout,
            proxies=proxies,
            api_version=api_version,
            serializer=serializer,
        )
        self.expression = expression

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes MSGraphSDKEvaluateTrigger arguments and classpath."""
        name, fields = super().serialize()
        fields = {**{"expression": self.expression}, **fields}
        return name, fields

    def evaluator(self) -> ExpressionEvaluator:
        return ExpressionEvaluator(self.get_conn())

    async def execute(self) -> AsyncIterator[TriggerEvent]:
        return await self.evaluator().evaluate(self.expression)


class MSGraphSDKSendAsyncTrigger(MSGraphSDKBaseTrigger):
    """
    A Microsoft Graph API trigger which allows you to execute an async URL request using the msgraph_sdk client.

    https://github.com/microsoftgraph/msgraph-sdk-python

    :param url: The url being executed on the msgraph_sdk client (templated).
    :param response_type: The response_type being returned by the msgraph_sdk client.
    :param method: The HTTP method being used by the msgraph_sdk client (default is GET).
    :param conn_id: The HTTP Connection ID to run the trigger against (templated).
    :param timeout: The HTTP timeout being used by the msgraph_sdk client (default is None).
        When no timeout is specified or set to None then no HTTP timeout is applied on each request.
    :param proxies: A Dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the msgraph_sdk client to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string as "v1.0" or "beta".
        This will determine which msgraph_sdk client is going to be used as each version has a dedicated client.
    """

    DEFAULT_HEADERS = {"Accept": "application/json;q=1"}
    template_fields: Sequence[str] = (
        "url",
        "response_type",
        "path_parameters",
        "url_template",
        "query_parameters",
        "headers",
        "content",
        "conn_id",
    )

    def __init__(
        self,
        url: Optional[str] = None,
        response_type: Optional[ResponseType] = None,
        path_parameters: Optional[Dict[str, Any]] = None,
        url_template: Optional[str] = None,
        method: str = "GET",
        query_parameters: Optional[Dict[str, QueryParams]] = None,
        headers: Optional[Dict[str, str]] = None,
        content: Optional[BytesIO] = None,
        conn_id: str = DEFAULT_CONN_NAME,
        timeout: Optional[float] = None,
        proxies: Optional[Dict] = None,
        api_version: Union[APIVersion, str] = APIVersion.v1,
        serializer: Union[str, Type[ResponseSerializer]] = ResponseSerializer,
    ):
        super().__init__(
            conn_id=conn_id,
            timeout=timeout,
            proxies=proxies,
            api_version=api_version,
            serializer=serializer,
        )
        self.url = url
        self.response_type = response_type
        self.path_parameters = path_parameters
        self.url_template = url_template
        self.method = method
        self.query_parameters = query_parameters
        self.headers = headers
        self.content = content

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serializes MSGraphSDKAsyncSendTrigger arguments and classpath."""
        name, fields = super().serialize()
        fields = {
            **{
                "url": self.url,
                "path_parameters": self.path_parameters,
                "url_template": self.url_template,
                "method": self.method,
                "query_parameters": self.query_parameters,
                "headers": self.headers,
                "content": self.content,
                "response_type": self.response_type,
            },
            **fields,
        }
        return name, fields

    def normalize_url(self) -> str:
        if self.url.startswith("/"):
            return self.url.replace("/", "", 1)
        return self.url

    def request_information(self) -> RequestInformation:
        request_information = RequestInformation()
        if self.url.startswith("http"):
            request_information.url = self.url
        else:
            request_information.url_template = f"{{+baseurl}}/{self.normalize_url()}"
        request_information.path_parameters = self.path_parameters or {}
        request_information.http_method = Method(self.method.strip().upper())
        request_information.query_parameters = self.query_parameters or {}
        request_information.content = self.content
        headers = (
            {**self.DEFAULT_HEADERS, **self.headers}
            if self.headers
            else self.DEFAULT_HEADERS
        )
        for header_name, header_value in headers.items():
            request_information.headers.try_add(
                header_name=header_name, header_value=header_value
            )
        return request_information

    @cached_property
    def data_error_type(self) -> ParsableFactory:
        return ODATA_ERROR_TYPE[self.api_version]

    def error_mapping(self) -> Dict[str, Optional[ParsableFactory]]:
        return {
            "4XX": self.data_error_type,
            "5XX": self.data_error_type,
        }

    def get_parsable_factory(self) -> Optional[Union[ParsableFactory, str]]:
        if self.response_type:
            with suppress(ImportError, TypeError):
                parsable_factory = import_string(self.response_type)
                if issubclass(parsable_factory, Parsable):
                    return parsable_factory
        return None

    async def execute(self) -> AsyncIterator[TriggerEvent]:
        parsable_factory = self.get_parsable_factory()

        self.log.debug("parsable factory is %s", parsable_factory)

        if parsable_factory:
            return await self.request_adapter.send_async(
                request_info=self.request_information(),
                parsable_factory=parsable_factory,
                error_map=self.error_mapping(),
            )
        return await self.request_adapter.send_primitive_async(
            request_info=self.request_information(),
            response_type=self.response_type,
            error_map=self.error_mapping(),
        )
