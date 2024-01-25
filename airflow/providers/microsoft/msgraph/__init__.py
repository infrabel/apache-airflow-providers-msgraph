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

from typing import TypeVar

# pylint: disable=W0406
import msgraph
import msgraph_beta

# noqa: E501
from airflow.providers.microsoft.msgraph.version import __version__

DEFAULT_CONN_NAME = "msgraph_default"
CLIENT_TYPE: TypeVar = TypeVar(
    "CLIENT_TYPE",
    msgraph.GraphServiceClient,  # pylint: disable=E1101
    msgraph_beta.GraphServiceClient,  # pylint: disable=E1101
)


def get_provider_info():
    # Specification: https://github.com/apache/airflow/blob/main/airflow/provider_info.schema.json
    return {
        "package-name": "apache-airflow-providers-msgraph",
        "name": "Airflow Provider Microsoft Graph API Operators",
        "description": "Airflow provider package for Microsoft Graph API Operators.",
        "versions": [__version__],
    }


__all__ = ["get_provider_info"]
