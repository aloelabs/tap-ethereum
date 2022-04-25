"""Stream type classes for tap-ethereum."""

from asyncio import events
from pathlib import Path
from re import L
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import Stream, typing as th  # JSON Schema typing helpers

from tap_ethereum.client import EthereumStream
from web3.types import ABIEvent

from tap_ethereum.typing import AddressType

# TODO: Delete this is if not using json files for schema definition
SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
# TODO: - Override `UsersStream` and `GroupsStream` with your own stream definition.
#       - Copy-paste as many times as needed to create multiple stream types.


class BlocksStream(EthereumStream):
    name = "blocks"

    pass


class EventStream(EthereumStream):
    name = "events"

    abi: ABIEvent = None

    def __init__(self, *args, **kwargs):
        # cache file_config so we dont need to go iterating the config list again later
        self.abi = kwargs.pop("abi")
        self.address = kwargs.pop("address")
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        # namespace table somehow
        return 'events_' + self.abi.get('name')

    # todo: create a mapping from schema to
    @staticmethod
    def get_jsonschema_type(abi_type: str) -> th.JSONTypeHelper:
        if abi_type == 'address':
            return AddressType
        else:
            return th.StringType

    @property
    def schema(self) -> dict:
        properties: List[th.Property] = []

        properties.append(th.Property('address', AddressType, required=True))

        for input_abi in self.abi.get('inputs'):
            properties.append(th.Property(input_abi.get(
                'name'), self.get_jsonschema_type(input_abi.get('type')), required=True))

        return th.PropertiesList(*properties).to_dict()


class GettersStream(EthereumStream):
    name = "getters"

    # parent_stream_type = BlocksStream

    def __init__(self, *args, **kwargs):
        # cache file_config so we dont need to go iterating the config list again later
        self.abi = kwargs.pop("abi")
        self.address = kwargs.pop("address")
        self.contract = kwargs.pop('contract')

        super().__init__(*args, **kwargs)
