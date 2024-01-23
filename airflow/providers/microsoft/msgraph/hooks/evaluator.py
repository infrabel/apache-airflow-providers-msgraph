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
import dataclasses
import inspect
import re
from typing import TYPE_CHECKING

from dacite import from_dict

if TYPE_CHECKING:
    from airflow.providers.microsoft.msgraph import CLIENT_TYPE


class ExpressionEvaluator:
    attributes_pattern = re.compile(r"\.(?![^\(]*\))")
    attribute_pattern = re.compile(r'["\']([^"\']*)["\']')

    def __init__(self, client: CLIENT_TYPE):
        self.client = client

    def get_argument(self, arg):
        if isinstance(arg, ast.Dict):
            return ast.literal_eval(arg)
        return self.attribute_pattern.sub(lambda match: match.group(1), arg.value)

    def get_arguments(self, attribute: str):
        if "(" in attribute and ")" in attribute:
            method_name, raw_args = attribute.split("(")
            args = [
                self.get_argument(arg)
                for arg in ast.parse(f"dummy({raw_args[:-1]})").body[0].value.args
            ]
            return method_name, args
        return attribute, None

    def evaluate_arguments(self, target, method, args):
        inner_types = dict(target.__class__.__dict__)
        method_signature = inspect.signature(method)
        for index, param in enumerate(method_signature.parameters.values()):
            annotation = ast.parse(param.annotation, mode="eval")
            if isinstance(annotation.body, ast.Subscript):
                if isinstance(annotation.body.slice, ast.Name):
                    param_type = inner_types.get(annotation.body.slice.id)
                else:
                    param_type = inner_types.get(annotation.body.slice.value.id)
            else:
                param_type = inner_types.get(annotation.body.id)
            if dataclasses.is_dataclass(param_type):
                args[index] = from_dict(data_class=param_type, data=args[index])
        return args

    def invoke(self, target, attribute: str):
        method_name, args = self.get_arguments(attribute)
        if args is not None:
            if len(args) > 0:
                method = getattr(target, method_name)
                return method(*self.evaluate_arguments(target, method, args))
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
