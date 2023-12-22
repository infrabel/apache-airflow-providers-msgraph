from assertpy import assert_that
from kiota_serialization_json.json_parse_node import JsonParseNode
from msgraph.generated.users.delta.delta_get_response import DeltaGetResponse

from airflow.providers.microsoft.msgraph.triggers.serializer import ResponseSerializer
from tests.unit.base import BaseTestCase


class ResponseSerializerTestCase(BaseTestCase):
    def test_serialize(self):
        response = JsonParseNode(self.load_json("resources", "users.json")).get_object_value(DeltaGetResponse)

        actual = ResponseSerializer.serialize(response)

        assert_that(actual).is_type_of(str)
        assert_that(actual).is_equal_to(self.load_file("resources", "users.json"))
