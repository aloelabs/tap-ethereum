"""Ethereum tap class."""

from copy import deepcopy
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
    EventsStream,
    GetterStream,
)
from tap_ethereum.typing import AddressType


class TapEthereum(Tap):
    """Ethereum tap class."""
    name = "tap-ethereum"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
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
                        "instances",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property(
                                    "address",
                                    AddressType,
                                    description="Address of the deployed smart contract."
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
                        "abi",
                        th.StringType,
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
            "ethereum_rpc",
            th.URIType,
            description="URI for a HTTP based JSON-RPC server",
            required=True,
        ),
        th.Property(
            "confirmations",
            th.IntegerType,
            description="Number of confirmations to wait before polling a new block",
            default=12,
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            default=50
        ),
        th.Property(
            "concurrency",
            th.IntegerType,
            default=50
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType())
    ).to_dict()

    @property
    def etherscan(self) -> Etherscan:
        return Etherscan(self.config.get("etherscan_api_key"))

    def load_abi(self, contract_config: Mapping[str, Any]) -> Contract:
        if contract_config.get('abi'):
            with open(contract_config.get('abi'), 'r') as abi_file:
                abi_json = abi_file.read()
        else:
            abi_json = self.etherscan.get_contract_abi(
                address=contract_config.get("instances")[0]["address"])

        abi = json.loads(abi_json)
        return abi

    def _find_getter_abi_by_name(self, getter_name: str, abi: dict) -> dict:
        return next((description for description in abi if description.get("type")
                     == "function" and description.get("name") == getter_name), None)

    def _find_event_abi_by_name(self, event_name: str, abi: dict) -> dict:
        return next((description for description in abi if description.get("type")
                     == "event" and description.get("name") == event_name), None)

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        streams: List[Stream] = []

        for contract_config in self.config.get('contracts', []):
            abi = self.load_abi(contract_config)

            for getter_name in contract_config.get('getters', []):
                getter_abi = self._find_getter_abi_by_name(getter_name, abi)
                streams.append(GetterStream(
                    tap=self,
                    abi=getter_abi,
                    contract_instances=contract_config.get('instances'),
                    contract_name=contract_config.get('name'),
                ))

            for event_name in contract_config.get('events', []):
                event_abi = self._find_event_abi_by_name(event_name, abi)
                streams.append(EventsStream(
                    tap=self,
                    abi=event_abi,
                    contract_instances=contract_config.get('instances'),
                    contract_name=contract_config.get('name'),
                ))

        return streams
