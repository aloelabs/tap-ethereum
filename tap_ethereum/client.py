"""Custom client handling, including EthereumStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk.tap_base import Tap
from singer_sdk.streams import Stream

from web3 import Web3
from etherscan import Etherscan

# declare a away to store contracts somehow


class EthereumStream(Stream):
    """Stream class for Ethereum streams."""

    def __init__(self, tap: Tap):
        super().__init__(tap)
        # TODO: support other providers
        self.web3 = Web3(Web3.WebsocketProvider(self.config.get("rpc_endpoint_uri")))

        if self.config.get("etherscan_api_key"):
            self.etherscan = Etherscan(self.config.get("etherscan_api_key"))

        self.contracts = []
        for conntract in self.config.get('contracts'):
            if contract_config.get("abi")

            contract = self.web3.eth.contract(address=contract_config.get(''))

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
