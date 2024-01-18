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

import json
from typing import Dict, Optional, Any, TYPE_CHECKING, Sequence, Union, List

from airflow import AirflowException
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import BaseOperator
from airflow.providers.microsoft.msgraph import DEFAULT_CONN_NAME
from airflow.providers.microsoft.msgraph.triggers.msgraph import (
    MSGraphSDKEvaluateTrigger,
    MSGraphSDKAsyncSendTrigger,
)
from airflow.utils import timezone
from msgraph_core import APIVersion

if TYPE_CHECKING:
    from airflow.utils.context import Context


class MSGraphSDKAsyncOperator(BaseOperator):
    """
    A Microsoft Graph API operator which allows you to execute an expression on the msgraph_sdk client.

    https://github.com/microsoftgraph/msgraph-sdk-python

    :param expression: The expression being executed on the msgraph_sdk client (templated).
    :param conn_id: The HTTP Connection ID to run the operator against (templated).
    :param trigger_dag_id: The DAG ID to be triggered on each event (templated).
    :param timeout: The HTTP timeout being used by the msgraph_sdk client (default is None).
        When no timeout is specified or set to None then no HTTP timeout is applied on each request.
    :param proxies: A Dict defining the HTTP proxies to be used (default is None).
    :param api_version: The API version of the msgraph_sdk client to be used (default is v1).
        You can pass an enum named APIVersion which has 2 possible members v1 and beta,
        or you can pass a string which equals the enum values as "v1.0" or "beta".
        This will determine which msgraph_sdk client is going to be used as each version has a dedicated client.
    """

    template_fields: Sequence[str] = ("expression", "conn_id", "trigger_dag_id")

    def __init__(
        self,
        *,
        expression: Optional[str],
        conn_id: str = DEFAULT_CONN_NAME,
        trigger_dag_id: Optional[str] = None,
        timeout: Optional[float] = None,
        proxies: Optional[Dict] = None,
        api_version: Union[APIVersion, str] = APIVersion.v1,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.expression = expression
        self.conn_id = conn_id
        self.trigger_dag_id = trigger_dag_id
        self.timeout = timeout
        self.proxies = proxies
        self.api_version = api_version
        self.results: List[Dict[Any, Any]] = []

    def execute(self, context: Context) -> None:
        self.defer(
            trigger=MSGraphSDKEvaluateTrigger(
                expression=self.expression,
                conn_id=self.conn_id,
                timeout=self.timeout,
                proxies=self.proxies,
                api_version=self.api_version,
            ),
            method_name="execute_complete",
        )

    def execute_complete(
        self, context: Context, event: Optional[Dict[Any, Any]] = None
    ) -> Any:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            self.log.info(
                "%s completed with %s: %s", self.task_id, event.get("status"), event
            )

            if event.get("status") == "failure":
                raise AirflowException(event.get("message"))

            response = event.get("response")

            self.log.info("response: %s", response)

            if response:
                response = json.loads(response)
                event["response"] = response

                if self.trigger_dag_id:
                    dag_run = trigger_dag(
                        dag_id=self.trigger_dag_id,
                        conf=event,
                        execution_date=timezone.utcnow(),
                    )

                    self.log.info(
                        "Dag %s was triggered: %s", self.trigger_dag_id, dag_run
                    )
                elif self.do_xcom_push:
                    self.results.append(response)

                odata_next_link = response.get("@odata.nextLink")
                response_type = event.get("type")

                self.log.info("odata_next_link: %s", odata_next_link)
                self.log.info("response_type: %s", response_type)

                if odata_next_link and response_type:
                    self.defer(
                        trigger=MSGraphSDKAsyncSendTrigger(
                            url=odata_next_link,
                            response_type=response_type,
                            conn_id=self.conn_id,
                            timeout=self.timeout,
                            proxies=self.proxies,
                            api_version=self.api_version,
                        ),
                        method_name="execute_complete",
                    )

                if self.do_xcom_push and self.results:
                    if len(self.results) == 1:
                        return self.results[0]
                    return self.results
        return None
