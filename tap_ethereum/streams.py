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

from tap_ethereum.client import EthereumStream
from web3.types import ABIEvent
from web3.eth import Contract

from tap_ethereum.typing import AddressType
from datetime import datetime

import subprocess

# TODO: how to deal with uncles?

# TODO: download this from somewhere?

# pull out process block
# commonality is that it processes every block


class GetterStream(EthereumStream):
    contract_name: str = None

    output_labels: List[str] = []

    abi: dict

    STATE_MSG_FREQUENCY = 10

    replication_key = "block_number"

    address_to_start_block: Dict[str, int] = {}

    def __init__(self, *args, **kwargs):
        self.contract_name = kwargs.pop("contract_name")

        self.abi = kwargs.pop("abi")
        for index, output_abi in enumerate(self.abi.get('outputs')):
            self.output_labels.append(output_abi.get('name') or index)

        for instance in kwargs.pop("instances"):
            self.address_to_start_block[instance["address"]] = instance["start_block"]

        super().__init__(*args, **kwargs)

    @property
    def getter_name(self) -> str:
        return self.abi.get('name')

    @property
    def name(self) -> str:
        # namespace table somehow
        return f"{self.contract_name}_getters_{self.getter_name}"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:

        address = context.get('address')
        start_block = self.address_to_start_block[address]

        cmd = ['./block-gobbler/bin/dev', 'getters',
               '--rpc', self.config.get("ethereum_rpc"),
               '--abi', json.dumps([self.abi]),
               '--address', address,
               '--getter', self.getter_name,
               '--startBlock', str(start_block),
               '--endBlock', str(start_block + 1000),
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
    def partitions(self) -> List[dict]:
        return [{"address": address} for address in self.address_to_start_block.keys()]

    @property
    def schema(self) -> dict:
        properties: List[th.Property] = []

        properties.append(th.Property('address', AddressType, required=True))
        properties.append(th.Property('block_number', th.IntegerType, required=True))

        outputs_properties: List[th.Property] = []
        for index, output_abi in enumerate(self.abi.get('outputs')):
            properties.append(th.Property(self.output_labels[index], get_jsonschema_type(
                output_abi.get('type')), required=True))
        outputs_type = th.ObjectType(*outputs_properties)
        properties.append(th.Property('outputs', outputs_type, required=True))

        return th.PropertiesList(*properties).to_dict()


def get_jsonschema_type(abi_type: str) -> th.JSONTypeHelper:
    if abi_type == 'address':
        return AddressType
    else:
        return th.StringType
