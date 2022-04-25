"""Ethereum tap class."""

from array import ArrayType
from typing import List
from pkg_resources import require

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers
# TODO: Import your custom stream types here:
from tap_ethereum.streams import (
    EthereumStream,
    UsersStream,
)
# TODO: Compile a list of custom stream types here
#       OR rewrite discover_streams() below with your custom logic.
STREAM_TYPES = [
    UsersStream,
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
                        "name",
                        th.StringType,
                        description="Name of the contract"
                    ),
                    # TODO: create custom address type
                    th.Property(
                        "address",
                        th.StringType,
                        description="Address of the smart contract (defaults to fetching all matching events)"
                    ),
                    th.Property(
                        "abi",
                        th.ObjectType(
                            th.Property(
                                "name",
                                th.StringType,
                            ),
                            th.Property(
                                "file",
                                th.StringType,
                            )
                        ),
                        description="ABI of the contract (defaults to fetching ABI Etherscan if not provided)"
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
                    )
                )
            ),
            required=True
        ),
        th.Property(
            "etherscan_api_key",
            th.StringType,
            description="API key for Etherscan paid plan (optional)"
        )
    ).to_dict()

    def _initialize_web3_provider():
        pass

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]
