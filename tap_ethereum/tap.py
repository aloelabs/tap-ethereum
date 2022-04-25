"""Ethereum tap class."""

from web3.eth import Contract
import json
from etherscan import Etherscan
from web3 import Web3
from array import ArrayType
from typing import Any, List, Mapping
from pkg_resources import require

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_ethereum.streams import (
    EventStream,
    BlocksStream,
    GettersStream,
)
from tap_ethereum.typing import AddressType


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
    def load_contract(self, contract_config: Mapping[str, Any]) -> Contract:
        address = contract_config.get('address')
        if contract_config.get('abi'):
            with open(contract_config.get('file'), 'r') as abi_file:
                abi_json = abi_file.read()
        elif address:
            abi_json = self.etherscan.get_contract_abi(address=address)
        else:
            self.logger.error("Missing ABI file and contract address.")
            exit(1)

        abi = json.loads(abi_json)
        contract = self.web3.eth.contract(abi=abi, address=address)
        return contract

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        streams: List[Stream] = []

        streams.append()

        for contract_config in self.config.get('contracts'):
            contract = self.load_contract(contract_config)

            events_abi = contract.events._events

            if contract_config.get('events'):
                events_abi = filter(lambda event_abi: event_abi.get(
                    'name') in contract_config.get('events'), events_abi)

            for event_abi in events_abi:
                stream = EventStream(
                    tap=self,
                    abi=event_abi,
                    web3=self.web3,
                    address=contract.address,
                )
                streams.append(stream)

            getters_abi = filter(lambda function_abi: function_abi.get(
                'stateMutability') == 'view', contract.all_functions())

            if contract_config.get('address'):
                if contract_config.get('getters'):
                    getters_abi = filter(lambda getter_abi: getter_abi.get(
                        'name') in contract_config.get('getters'), getters_abi)

                stream = GettersStream(
                    tap=self,
                    abi=getters_abi,
                    contract=contract,
                    address=contract.address,
                )

        return streams
