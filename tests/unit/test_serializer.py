from airflow.providers.microsoft.msgraph.triggers.serializer import ResponseSerializer
from assertpy import assert_that
from kiota_serialization_json.json_parse_node import JsonParseNode
from msgraph.generated.users.delta.delta_get_response import DeltaGetResponse

from tests.unit.base import BaseTestCase
from tests.unit.conftest import load_json, load_file


class ResponseSerializerTestCase(BaseTestCase):
    def test_serialize_when_parsable_type(self):
        response = JsonParseNode(load_json("resources", "users.json")).get_object_value(DeltaGetResponse)

        actual = ResponseSerializer.serialize(response)

        assert_that(actual).is_type_of(str).is_equal_to(load_file("resources", "users.json"))

    def test_serialize_when_bytes(self):
        response = load_file("resources", "users.json").encode()

        actual = ResponseSerializer.serialize(response)

        assert_that(actual).is_type_of(bytes).is_same_as(response)
