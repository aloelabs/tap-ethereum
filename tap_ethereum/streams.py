"""Stream type classes for tap-ethereum."""

from asyncio import events
from audioop import add
import json
from pathlib import Path
from re import L
from tracemalloc import start
from typing import Any, Dict, Optional, Union, List, Iterable
from pendulum import datetime

from singer_sdk import Stream, typing as th  # JSON Schema typing helpers

from web3.types import ABIEvent
from web3.eth import Contract

from tap_ethereum.typing import AddressType
from datetime import datetime

import subprocess

# TODO: how to deal with uncles?

# TODO: download this from somewhere?

# pull out process block
# commonality is that it processes every block


class ContractStream(Stream):
    contract_name: str
    address_to_start_block: Dict[str, int] = {}

    def __init__(self, *args, **kwargs):
        self.contract_name = kwargs.pop("contract_name")

        for instance in kwargs.pop("contract_instances"):
            self.address_to_start_block[instance["address"]] = instance["start_block"]

        super().__init__(*args, **kwargs)

    @property
    def partitions(self) -> List[dict]:
        return [{"address": address} for address in self.address_to_start_block.keys()]


class GetterStream(ContractStream):
    output_labels: List[str] = []

    abi: dict

    STATE_MSG_FREQUENCY = 10

    replication_key = "block_number"

    def __init__(self, *args, **kwargs):
        self.abi = kwargs.pop("abi")
        for index, output_abi in enumerate(self.abi.get('outputs')):
            self.output_labels.append(output_abi.get('name') or index)

        super().__init__(*args, **kwargs)

    @property
    def getter_name(self) -> str:
        return self.abi.get('name')

    @property
    def name(self) -> str:
        return f"{self.contract_name}_getters_{self.getter_name}"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        address = context.get('address')
        start_block = max(self.get_starting_replication_key_value(
            context) or 0, self.address_to_start_block[address])

        cmd = ['./block-gobbler/bin/dev', 'getters',
               '--rpc', self.config.get("ethereum_rpc"),
               '--abi', json.dumps([self.abi]),
               '--address', address,
               '--getter', self.getter_name,
               '--startBlock', str(start_block),
               #    '--endBlock', str(start_block + 1000),
               '--confirmations', str(self.config.get('confirmations')),
               '--batchSize', str(self.config.get('batch_size')),
               '--concurrency', str(self.config.get('concurrency'))]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for line in iter(proc.stdout.readline, ""):
            block_number, outputs = json.loads(line.strip())
            row = dict(
                address=context.get('address'),
                block_number=block_number,
                outputs={label: value for (label, value) in zip(
                    self.output_labels, outputs)}
            )
            yield row

    @property
    def schema(self) -> dict:
        properties: List[th.Property] = []

        properties.append(th.Property('address', AddressType, required=True))
        properties.append(th.Property('block_number', th.IntegerType, required=True))

        outputs_properties: List[th.Property] = []
        for index, output_abi in enumerate(self.abi.get('outputs')):
            outputs_properties.append(th.Property(self.output_labels[index], get_jsonschema_type(
                output_abi.get('type')), required=True))
        outputs_type = th.ObjectType(*outputs_properties)
        properties.append(th.Property('outputs', outputs_type, required=True))

        return th.PropertiesList(*properties).to_dict()


class EventsStream(ContractStream):
    abi: dict

    STATE_MSG_FREQUENCY = 10

    replication_key = "block_number"

    input_labels: List[str] = []

    def __init__(self, *args, **kwargs):
        self.abi = kwargs.pop("abi")

        for index, input_abi in enumerate(self.abi.get('inputs')):
            # TODO: figure out if name is required on events
            self.input_labels.append(input_abi.get('name'))

        super().__init__(*args, **kwargs)

    @property
    def event_name(self) -> str:
        return self.abi.get('name')

    @property
    def name(self) -> str:
        return f"{self.contract_name}_events_{self.event_name}"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        address = context.get('address')
        start_block = max(self.get_starting_replication_key_value(
            context) or 0, self.address_to_start_block[address])

        cmd = ['./block-gobbler/bin/dev', 'events',
               '--rpc', self.config.get("ethereum_rpc"),
               '--abi', json.dumps([self.abi]),
               '--address', address,
               '--event', self.event_name,
               '--startBlock', str(start_block),
               #    '--endBlock', str(start_block + 1000),
               '--confirmations', str(self.config.get('confirmations')),
               '--concurrency', str(self.config.get('concurrency'))]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for line in iter(proc.stdout.readline, ""):
            event_data = json.loads(line.strip())
            row = dict(
                address=context.get('address'),
                block_number=event_data.get('blockNumber'),
                inputs={label: event_data["returnValues"][label]
                        for label in self.input_labels}
            )
            yield row

    @property
    def schema(self) -> dict:
        properties: List[th.Property] = []

        properties.append(th.Property('address', AddressType, required=True))
        properties.append(th.Property('block_number', th.IntegerType, required=True))

        # TODO: do we keep nesting or not?
        inputs_properties: List[th.Property] = []
        for input_abi in self.abi.get('inputs'):
            inputs_properties.append(th.Property(input_abi.get("name"), get_jsonschema_type(
                input_abi.get('type')), required=True))
        inputs_type = th.ObjectType(*inputs_properties)
        properties.append(th.Property('inputs', inputs_type, required=True))

        return th.PropertiesList(*properties).to_dict()


def get_jsonschema_type(abi_type: str) -> th.JSONTypeHelper:
    if abi_type == 'address':
        return AddressType
    else:
        return th.StringType
