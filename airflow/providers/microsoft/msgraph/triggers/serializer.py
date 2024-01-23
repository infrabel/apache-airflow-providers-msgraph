import json
from typing import Optional
from uuid import UUID

import pendulum
from kiota_abstractions.serialization import Parsable
from kiota_serialization_json.json_serialization_writer import JsonSerializationWriter


class ResponseSerializer:
    @classmethod
    def serialize(cls, response) -> Optional[str]:
        def convert(value) -> Optional[str]:
            if value is not None:
                if isinstance(value, UUID):
                    return str(value)
                if isinstance(value, pendulum.DateTime):
                    return value.to_iso8601_string()  # Adjust the format as needed
                raise TypeError(
                    f"Object of type {type(value)} is not JSON serializable!"
                )
            return None

        if response is not None:
            if isinstance(response, Parsable):
                writer = JsonSerializationWriter()
                response.serialize(writer)
                return json.dumps(writer.writer, default=convert)
            return response
        return None
