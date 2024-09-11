from typing import Any, List

import ujson
from eth_typing import ChecksumAddress
from eth_utils.address import to_checksum_address
from web3.contract.contract import Contract

import config

UNISWAP_V3_TICKLENS_ABI = ujson.loads(
    """
    [{"inputs":[{"internalType":"address","name":"pool","type":"address"},{"internalType":"int16","name":"tickBitmapIndex","type":"int16"}],"name":"getPopulatedTicksInWord","outputs":[{"components":[{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint128","name":"liquidityGross","type":"uint128"}],"internalType":"struct ITickLens.PopulatedTick[]","name":"populatedTicks","type":"tuple[]"}],"stateMutability":"view","type":"function"}]
    """
)
class TickLens:
    def __init__(
        self,
        address: ChecksumAddress | str,
        abi: List[Any] | None = None,
    ):
        self.address = to_checksum_address(address)
        self.abi = abi if abi is not None else UNISWAP_V3_TICKLENS_ABI

    @property
    def _w3_contract(self) -> Contract:
        return config.get_web3().eth.contract(
            address=self.address,
            abi=self.abi,
        )
