"""Ethereum tap class."""

from sqlalchemy import desc
import web3
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
    GetterStream,
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
            description="URI for a HTTP based JSON-RPC server",
            required=True,
        ),  # transactions? how to monitor every transaction? is it worth doing that?
        th.Property(
            "contracts",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "name",
                        th.StringType,
                        description="Name of the smart contract",
                        required=True,
                    ),
                    th.Property(
                        "addresses",
                        th.ArrayType(AddressType),
                        description="Addresses of the deployed smart contract. If not provided, fetches all events using ABI."
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
                )
            ),
            required=True
        ),
        th.Property(
            "etherscan_api_key",
            th.StringType,
            description="API key for Etherscan paid plan (optional)",
            default=""
        ),
        th.Property(
            "confirmations",
            th.IntegerType,
            description="Number of confirmations to wait before polling a new block",
            default=12,
        ),
        th.Property(
            "start_block",
            th.IntegerType,
            default=0,
            description="Block number to start fetching from"
        )
    ).to_dict()

    @property
    def web3(self) -> Web3:
        return Web3(Web3.HTTPProvider(self.config.get("rpc_endpoint_uri")))

    @property
    def etherscan(self) -> Etherscan:
        return Etherscan(self.config.get("etherscan_api_key"))

    def load_abi(self, contract_config: Mapping[str, Any]) -> Contract:
        addresses = contract_config.get('addresses')
        if contract_config.get('abi'):
            with open(contract_config.get('file'), 'r') as abi_file:
                abi_json = abi_file.read()
        elif addresses:
            abi_json = self.etherscan.get_contract_abi(address=addresses[0])
        else:
            self.logger.error("Missing ABI file and contract address.")
            exit(1)

        abi = json.loads(abi_json)
        return abi

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        streams: List[Stream] = []

        # streams.append(BlocksStream(tap=self, web3=self.web3,
        #                confirmations=self.config.get('confirmations'), start_block=self.config.get('start_block')))

        for contract_config in self.config.get('contracts'):
            contract_name = contract_config.get('name')
            abi = self.load_abi(contract_config)
            contract_class = self.web3.eth.contract(abi=abi)

            # events_abi = contract.events._events

            # if contract_config.get('events'):
            #     events_abi = filter(lambda event_abi: event_abi.get(
            #         'name') in contract_config.get('events'), events_abi)

            # for event_abi in events_abi:
            #     stream = EventStream(
            #         tap=self,
            #         abi=event_abi,
            #         web3=self.web3,
            #         contract_name=contract_name,
            #     )
            #     streams.append(stream)

            getters_abi = map(
                lambda contract_function: contract_function.abi, contract_class.all_functions())
            getters_abi = filter(lambda function_abi: function_abi.get(
                'stateMutability') == 'view', getters_abi)

            if contract_config.get('addresses'):
                contract_instances = []
                for address in contract_config.get('addresses'):
                    contract_instances.append(contract_class(address))
                    # TODO: support getters with inputs
                getters_abi = filter(lambda getter_abi: len(getter_abi.get(
                    'inputs')) == 0, getters_abi)
                if contract_config.get('getters'):
                    getters_abi = filter(lambda getter_abi: getter_abi.get(
                        'name') in contract_config.get('getters'), getters_abi)

                for getter_abi in getters_abi:
                    stream = GetterStream(
                        tap=self,
                        abi=getter_abi,
                        contracts=contract_instances,
                        web3=self.web3,
                        contract_name=contract_name,
                        confirmations=self.config.get('confirmations'),
                        start_block=self.config.get('start_block')
                    )
                    streams.append(stream)

        return streams
