"""Stream type classes for tap-ethereum."""

from asyncio import events
from pathlib import Path
from re import L
from tracemalloc import start
from typing import Any, Dict, Optional, Union, List, Iterable
from pendulum import datetime

from singer_sdk import Stream, typing as th  # JSON Schema typing helpers

from tap_ethereum.client import EthereumStream
from web3.types import ABIEvent
from web3.eth import Contract

from tap_ethereum.typing import AddressType
from datetime import datetime

# TODO: how to deal with uncles?

# TODO: download this from somewhere?

# pull out process block
# commonality is that it processes every block


class BlocksStream(EthereumStream):
    name = "blocks"

    confirmations: int = None

    replication_key = "number"

    STATE_MSG_FREQUENCY = 1

    start_block: int

    schema = th.PropertiesList(
        th.Property("timestamp", th.DateTimeType, required=True),
        th.Property("number", th.IntegerType, required=True),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        self.confirmations = kwargs.pop("confirmations")
        self.start_block = kwargs.pop("start_block")
        super().__init__(*args, **kwargs)

    def process_block(self, block_number: int, context: Optional[dict] = None) -> Optional[dict]:
        block = self.web3.eth.get_block(block_number)
        return dict(
            timestamp=datetime.fromtimestamp(block["timestamp"]),
            number=block["number"]
        )

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        # TODO: Write logic to extract data from the upstream source.
        # rows = mysource.getall()
        # for row in rows:
        #     yield row.to_dict()
        # raise NotImplementedError("The method is not yet implemented (TODO)")
        start_block_number = self.get_starting_replication_key_value(
            context) or self.start_block
        current_block_number = start_block_number + 1
        latest_block_number = self.web3.eth.get_block_number()

        while current_block_number < latest_block_number - self.confirmations:
            yield self.process_block(current_block_number, context=context)
            current_block_number += 1
            if current_block_number == latest_block_number - self.confirmations:
                latest_block_number = self.web3.eth.get_block_number()


class GetterStream(BlocksStream):
    contracts: Dict[str, Contract] = None
    contract_name: str = None

    def __init__(self, *args, **kwargs):
        self.abi = kwargs.pop("abi")
        contracts: List[Contract] = kwargs.pop("contracts")
        for contract in contracts:
            self.contracts[contract.address] = contract
        self.contract_name = kwargs.pop("contract_name")
        super().__init__(*args, **kwargs)

    @property
    def getter_name(self) -> str:
        return self.abi.get('name')

    @property
    def name(self) -> str:
        # namespace table somehow
        return f"{self.contract_name}_getters_{self.getter_name}"

    def process_block(self, block_number: int, context: Optional[dict] = None) -> Optional[dict]:
        contract = self.contracts[context.get('address')]
        response = contract.functions[self.getter_name]().call(
            block_identifier=block_number)
        print(response)

    @property
    def partitions(self) -> List[dict]:
        return [{"address": contract.address} for contract in self.contracts]

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
