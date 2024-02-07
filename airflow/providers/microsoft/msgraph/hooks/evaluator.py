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
from typing import TYPE_CHECKING, Optional

import regex
from dacite import from_dict
from simpleeval import SimpleEval

if TYPE_CHECKING:
    from airflow.providers.microsoft.msgraph import CLIENT_TYPE


class ExpressionEvaluator:
    attributes_pattern = regex.compile(r"\.(?![^\(]*\))")
    attribute_pattern = regex.compile(r"(\w+)\s*\(([^()]*((?:(?R)|[^()]+)*)+)\)")

    def __init__(self, client: CLIENT_TYPE):
        self.client = client

    def get_argument(self, arg):
        if isinstance(arg, ast.Dict):
            return ast.literal_eval(arg)
        return arg.value

    def get_arguments(self, attribute: str):
        match = self.attribute_pattern.match(attribute)
        if match:
            method_name = match.group(1)
            raw_args = match.group(2)
            accessor = self.get_accessor(attribute, method_name, raw_args)
            if raw_args is not None:
                if raw_args:
                    args = [
                        self.get_argument(arg)
                        for arg in ast.parse(f"dummy({raw_args})").body[0].value.args
                    ]
                    return method_name, args, accessor
                return method_name, [], accessor
            return method_name, None, accessor
        return attribute, None, None

    def get_accessor(
        self, attribute: str, method_name: str, raw_args: str
    ) -> Optional[str]:
        method_name_with_args = f"{method_name}({raw_args})"
        if attribute.startswith(method_name_with_args):
            accessor = attribute.replace(f"{method_name}({raw_args})", "")
            return accessor if accessor else None
        return None

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

    def invoke(self, args, method_name, target):
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
            method_name, args, accessor = self.get_arguments(attribute)
            target = self.invoke(args, method_name, target)

        method_name, args, accessor = self.get_arguments(attributes[-1])
        result = await self.invoke(args, method_name, target)

        if accessor:
            result = SimpleEval(names={"result": result}).eval(f"result{accessor}")
        return result
