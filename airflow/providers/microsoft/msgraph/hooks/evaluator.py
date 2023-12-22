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

import ast
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.providers.microsoft.msgraph import CLIENT_TYPE


class ExpressionEvaluator:
    def __init__(self, client: CLIENT_TYPE):
        self.client = client
        self.attributes_pattern = re.compile(r"\.(?![^\(]*\))")
        self.attribute_pattern = re.compile(r'["\']([^"\']*)["\']')

    def get_arguments(self, attribute: str):
        if "(" in attribute and ")" in attribute:
            method_name, raw_args = attribute.split("(")
            args = [
                self.attribute_pattern.sub(lambda match: match.group(1), arg.value)
                for arg in ast.parse(f"dummy({raw_args[:-1]})").body[0].value.args
            ]
            return method_name, args
        return attribute, None

    def invoke(self, target, attribute: str):
        method_name, args = self.get_arguments(attribute)
        if args is not None:
            if len(args) > 0:
                return getattr(target, method_name)(*args)
            return getattr(target, method_name)()
        return getattr(target, method_name)

    async def evaluate(self, expression: str):
        # Split the expression into individual attribute/method names
        attributes = self.attributes_pattern.split(expression)
        target = self.client

        for attribute in attributes[:-1]:
            target = self.invoke(target, attribute)

        result = await self.invoke(target, attributes[-1])

        return result
