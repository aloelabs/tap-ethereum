"""Stream type classes for tap-ethereum."""

from asyncio import events
from pathlib import Path
from re import L
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import Stream, typing as th  # JSON Schema typing helpers

from tap_ethereum.client import EthereumStream
from web3.types import ABIEvent
from web3.eth import Contract

from tap_ethereum.typing import AddressType


class BlocksStream(EthereumStream):
    name = "blocks"

    pass


def get_jsonschema_type(abi_type: str) -> th.JSONTypeHelper:
    if abi_type == 'address':
        return AddressType
    else:
        return th.StringType


class EventStream(EthereumStream):
    name = "events"

    abi: ABIEvent = None
    contract_name: str = None

    def __init__(self, *args, **kwargs):
        self.abi = kwargs.pop("abi")
        self.contract_name = kwargs.pop("contract_name")
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        # namespace table somehow
        return f"{self.contract_name}_events_{self.abi.get('name')}"

    @property
    def schema(self) -> dict:
        properties: List[th.Property] = []

        properties.append(th.Property('address', AddressType, required=True))

        for input_abi in self.abi.get('inputs'):
            properties.append(th.Property(input_abi.get(
                'name'), get_jsonschema_type(input_abi.get('type')), required=True))

        return th.PropertiesList(*properties).to_dict()


class GetterStream(EthereumStream):

    # parent_stream_type = BlocksStream

    contract: Contract = None
    contract_name: str = None

    def __init__(self, *args, **kwargs):
        self.abi = kwargs.pop("abi")
        self.address = kwargs.pop("address")
        self.contract_name = kwargs.pop("contract_name")
        super().__init__(*args, **kwargs)

    @property
    def name(self) -> str:
        # namespace table somehow
        return f"{self.contract_name}_getters_{self.abi.get('name')}"

    @property
    def schema(self) -> dict:
        properties: List[th.Property] = []

        properties.append(th.Property('address', AddressType, required=True))

        outputs_abi = self.abi.get('outputs')

        for index, output_abi in enumerate(self.abi.get('outputs')):
            getter_name = self.abi.get('name')
            output_name = output_abi.get('name')
            if not output_name:
                output_name = getter_name if len(
                    outputs_abi) == 1 else f"{getter_name}_{index}"
            properties.append(th.Property(output_name, get_jsonschema_type(
                output_abi.get('type')), required=True))

        return th.PropertiesList(*properties).to_dict()
