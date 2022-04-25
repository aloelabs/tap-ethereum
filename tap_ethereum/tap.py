"""Ethereum tap class."""

from web3.eth import Contract
import json
from etherscan import Etherscan
from web3 import Web3
from array import ArrayType
from typing import List
from pkg_resources import require

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_ethereum.streams import (
    EventStream,
)
from tap_ethereum.typing import AddressType

# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    EventStream,
]


# Just events to start


class TapEthereum(Tap):
    """Ethereum tap class."""
    name = "tap-ethereum"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "rpc_endpoint_uri",
            th.URIType,
            description="URI for a Websocket based JSON-RPC server",
            required=True,
        ),
        th.Property(
            "contracts",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "address",
                        AddressType,
                        description="Address of the smart contract (defaults to fetching all matching events)"
                    ),
                    th.Property(
                        "abi",
                        th.IntegerType,
                        description="ABI file of the contract (defaults to fetching ABI Etherscan if not provided)"
                    ),
                    th.Property(
                        "events",
                        th.ArrayType(th.StringType),
                        description="Events to track (defaults to tracking all smart contract events)"
                    ),
                    th.Property(
                        "getters",
                        th.ArrayType(th.StringType),
                        description="Getter functions to poll every block (defaults to tracking all getter functions)"
                    ),
                    th.Property(
                        "start_block",
                        th.IntegerType,
                        default=0,
                        description="Block number to start fetching from"
                    )
                )
            ),
            required=True
        ),
        th.Property(
            "etherscan_api_key",
            th.StringType,
            description="API key for Etherscan paid plan (optional)",
            default=""
        )
    ).to_dict()

    @property
    def web3(self) -> Web3:
        return Web3(Web3.WebsocketProvider(self.config.get("rpc_endpoint_uri")))

    @property
    def etherscan(self) -> Etherscan:
        return Etherscan(self.config.get("etherscan_api_key"))

    @property
    def contracts(self) -> List[Contract]:
        contracts: List[Contract] = []
        for contract_config in self.config.get('contracts'):
            address = contract_config.get('address')
            if contract_config.get('abi'):
                with open(contract_config.get('file'), 'r') as abi_file:
                    abi_json = abi_file.read()
            elif self.etherscan and address:
                abi_json = self.etherscan.get_contract_abi(address=address)
            else:
                self.logger.error("No CSV file defintions found.")
                exit(1)

            abi = json.loads(abi_json)

            contract = self.web3.eth.contract(abi=abi, address=address)
            contracts.append(contract)
        return contracts

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        streams: List[Stream] = []
        for contract in self.contracts:
            for event_abi in contract.events._events:
                stream = EventStream(
                    tap=self,
                    abi=event_abi,
                    web3=self.web3,
                    address=contract.address,
                )
                streams.append(stream)

        return streams
