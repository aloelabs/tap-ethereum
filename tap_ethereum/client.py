"""Custom client handling, including EthereumStream base class."""

from audioop import add
import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.tap_base import Tap
from singer_sdk.streams import Stream

from web3 import Web3
from etherscan import Etherscan
import json
from web3.eth import Contract

import os

# declare a away to store contracts somehow


class ContractWrapper:
    contract: Contract = None

    def __init__(self, contract: Contract, name: str = None) -> None:
        self.contract = contract
        self.name = name

    def getters(self) -> List['ContractFunction']:
        for f in self.contract.all_functions():
            print(f.abi)


class EthereumStream(Stream):
    """Stream class for Ethereum streams."""

    def __init__(self, tap: Tap):
        super().__init__(tap)
        # TODO: support other providers
        self.web3 = Web3(Web3.WebsocketProvider(self.config.get("rpc_endpoint_uri")))

        self.etherscan = None
        if self.config.get("etherscan_api_key"):
            self.etherscan = Etherscan(self.config.get("etherscan_api_key"))

        self.contracts = []
        for contract_config in self.config.get('contracts'):
            address = contract_config.get('address')
            if contract_config.get('abi'):
                abi_config = contract_config.get('abi')
                abi_name = abi_config.get('name')
                if abi_config.get('file'):
                    with open(abi_config.get('file'), 'r') as abi_file:
                        abi_json = abi_file.read()
            elif self.etherscan and address:
                abi_json = self.etherscan.get_contract_abi(address=address)
            else:
                raise Exception(f"Missing ABI for contract {contract_config}")

            abi = json.loads(abi_json)

            contract = self.web3.eth.contract(abi=abi, address=address)
            getters = filter(lambda f: f.abi.get('stateMutability')
                             == 'view', contract.all_functions())
            events = contract.events.abi
            # focus on this
    # def _parse_contract_config(config:)

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
        raise NotImplementedError("The method is not yet implemented (TODO)")
