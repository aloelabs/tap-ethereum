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


class EthereumStream(Stream):
    """Stream class for Ethereum streams."""

    def __init__(self, *args, **kwargs) -> None:
        self.web3 = kwargs.pop("web3")

        super().__init__(*args, **kwargs)

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
