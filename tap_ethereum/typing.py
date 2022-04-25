from singer_sdk.typing import JSONTypeHelper
from singer_sdk.helpers._classproperty import classproperty


class AddressType(JSONTypeHelper):
    @classproperty
    def type_dict(cls) -> dict:
        return {
            "type": ["string"],
            "pattern": "^0x[a-fA-F0-9]{40}$",
            "minLength": 42,
            "maxLength": 42
        }
