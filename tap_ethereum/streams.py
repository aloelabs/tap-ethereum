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

# TODO: how to deal with uncles?

# TODO: download this from somewhere?


class BlocksStream(EthereumStream):
    name = "blocks"

    confirmations: int = None

    schema = th.PropertiesList(
        th.Property("timestamp", th.IntegerType, required=True),
        th.Property("number", th.IntegerType, required=True),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        self.confirmations = kwargs.pop("confirmations")
        super().__init__(*args, **kwargs)


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

        inputs_properties: List[th.Property] = []
        for index, input_abi in enumerate(self.abi.get('inputs')):
            input_name = input_abi.get('name') or index
            inputs_properties.append(th.Property(input_name, get_jsonschema_type(
                input_abi.get('type')), required=True))
        inputs_type = th.ObjectType(*inputs_properties)

        # # TODO: clean up code
        properties.append(th.Property('inputs', inputs_type, required=True))

        return th.PropertiesList(*properties).to_dict()


class GetterStream(EthereumStream):
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

        outputs_properties: List[th.Property] = []
        for index, output_abi in enumerate(self.abi.get('outputs')):
            output_name = output_abi.get('name') or index
            properties.append(th.Property(output_name, get_jsonschema_type(
                output_abi.get('type')), required=True))
        outputs_type = th.ObjectType(*outputs_properties)
        properties.append(th.Property('outputs', outputs_type, required=True))

        return th.PropertiesList(*properties).to_dict()
