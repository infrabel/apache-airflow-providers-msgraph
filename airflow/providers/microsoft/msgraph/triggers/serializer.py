import json
from typing import Optional
from uuid import UUID

from kiota_abstractions.serialization import Parsable
from kiota_serialization_json.json_serialization_writer import JsonSerializationWriter


class ResponseSerializer:
    @classmethod
    def serialize(cls, response) -> Optional[str]:
        def uuid_converter(value) -> Optional[str]:
            if value is not None:
                if isinstance(value, UUID):
                    return str(value)
                raise TypeError(
                    f"Object of type {type(value)} is not JSON serializable!"
                )
            return None

        if response is not None:
            if isinstance(response, Parsable):
                writer = JsonSerializationWriter()
                response.serialize(writer)
                return json.dumps(writer.writer, default=uuid_converter)
            raise TypeError(
                f"Object of type {type(response)} should be an instance of type {Parsable.__name__}!"
            )
        return None
