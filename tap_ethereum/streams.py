"""Stream type classes for tap-ethereum."""

import json
from typing import Dict, Optional, List, Iterable

from singer_sdk import Stream, typing as th

from tap_ethereum.typing import AddressType

from stringcase import spinalcase

import subprocess

# TODO: unbundle inputs and stuff


class ContractStream(Stream):
    contract_name: str
    address_to_start_block: Dict[str, int] = {}

    def __init__(self, *args, **kwargs):
        self.contract_name = spinalcase(kwargs.pop("contract_name"))

        for instance in kwargs.pop("contract_instances"):
            self.address_to_start_block[instance["address"]] = instance["start_block"]

        super().__init__(*args, **kwargs)

    @property
    def partitions(self) -> List[dict]:
        return [{"address": address} for address in self.address_to_start_block.keys()]


class GetterStream(ContractStream):
    abi: dict

    STATE_MSG_FREQUENCY = 100

    replication_key = "block_number"

    primary_keys = ["address", "block_number"]

    def __init__(self, *args, **kwargs):
        self.abi = kwargs.pop("abi")

        super().__init__(*args, **kwargs)

    @property
    def output_labels(self) -> List[str]:
        return [output.get('name', index) for index, output in enumerate(self.abi.get('outputs'))]

    @property
    def flattened_output_labels(self) -> List[str]:
        return [f"outputs__{label}" for label in self.output_labels]

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

        cmd = ['block-gobbler', 'getters',
               '--rpc', self.config.get("ethereum_rpc"),
               '--abi', json.dumps([self.abi]),
               '--address', address,
               '--getter', self.getter_name,
               '--startBlock', str(start_block),
               '--confirmations', str(self.config.get('confirmations')),
               '--batchSize', str(self.config.get('batch_size')),
               '--concurrency', str(self.config.get('concurrency'))]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for line in iter(proc.stdout.readline, ""):
            block_number, outputs = json.loads(line.strip())
            row = dict(
                address=context.get('address'),
                block_number=block_number,
                **{label: value for (label, value) in zip(
                    self.flattened_output_labels, outputs)}
            )
            yield row

    @property
    def schema(self) -> dict:
        properties: List[th.Property] = []

        properties.append(th.Property('address', AddressType, required=True))
        properties.append(th.Property('block_number', th.IntegerType, required=True))

        for index, output_abi in enumerate(self.abi.get('outputs')):
            properties.append(th.Property(self.flattened_output_labels[index], get_jsonschema_type(
                output_abi.get('type')), required=True))

        return th.PropertiesList(*properties).to_dict()


class EventsStream(ContractStream):
    abi: dict

    STATE_MSG_FREQUENCY = 100

    replication_key = "block_number"

    primary_keys = ["block_number", "log_index"]

    def __init__(self, *args, **kwargs):
        self.abi = kwargs.pop("abi")

        for index, input_abi in enumerate(self.abi.get('inputs')):
            # TODO: figure out if name is required on events
            self.input_labels.append(input_abi.get('name'))

        super().__init__(*args, **kwargs)

    @ property
    def event_name(self) -> str:
        return self.abi.get('name')

    @ property
    def name(self) -> str:
        return f"{self.contract_name}_events_{self.event_name}"

    @property
    def input_labels(self) -> List[str]:
        return [input_.get('name') for input_ in self.abi.get('inputs')]

    @property
    def flattened_input_labels(self) -> List[str]:
        return [f"inputs__{label}" for label in self.input_labels]

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        address = context.get('address')
        start_block = max(self.get_starting_replication_key_value(
            context) or 0, self.address_to_start_block[address])

        cmd = ['block-gobbler', 'events',
               '--rpc', self.config.get("ethereum_rpc"),
               '--abi', json.dumps([self.abi]),
               '--address', address,
               '--event', self.event_name,
               '--startBlock', str(start_block),
               '--confirmations', str(self.config.get('confirmations')),
               '--concurrency', str(self.config.get('concurrency'))]

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
        for line in iter(proc.stdout.readline, ""):
            event_data = json.loads(line.strip())
            row = dict(
                address=context.get('address'),
                block_number=event_data.get('blockNumber'),
                **{flattened_label: event_data["returnValues"][label]
                   for label, flattened_label in zip(self.input_labels, self.flattened_input_labels)},
                log_index=event_data.get("logIndex")
            )
            yield row

    @ property
    def schema(self) -> dict:
        properties: List[th.Property] = []

        properties.append(th.Property('address', AddressType, required=True))
        properties.append(th.Property('block_number', th.IntegerType, required=True))
        properties.append(th.Property('log_index', th.IntegerType, required=True,
                                      description="Integer of the event index position in the block"))

        for index, input_abi in enumerate(self.abi.get('inputs')):
            properties.append(th.Property(self.flattened_input_labels[index], get_jsonschema_type(
                input_abi.get('type')), required=True))

        return th.PropertiesList(*properties).to_dict()


def get_jsonschema_type(abi_type: str) -> th.JSONTypeHelper:
    if abi_type == 'address':
        return AddressType
    else:
        return th.StringType
