# TODO: support unwinding updates for re-org


from io import TextIOWrapper
from typing import Any, Dict, List, TextIO, Tuple

import ujson
from eth_typing import ChecksumAddress
from eth_utils.address import to_checksum_address
from web3 import Web3
from web3._utils.events import get_event_data
from web3._utils.filters import construct_event_filter_params
import config
from .logging import logger
from .v3_dataclasses import (
    UniswapV3BitmapAtWord,
    UniswapV3LiquidityAtTick,
    UniswapV3LiquidityEvent,
    UniswapV3PoolExternalUpdate,
)

UNISWAP_V3_POOL_ABI = ujson.loads(
    """
    [{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"CollectProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid1","type":"uint256"}],"name":"Flash","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextOld","type":"uint16"},{"indexed":false,"internalType":"uint16","name":"observationCardinalityNextNew","type":"uint16"}],"name":"IncreaseObservationCardinalityNext","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Initialize","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"feeProtocol0Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol0New","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1New","type":"uint8"}],"name":"SetFeeProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collect","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collectProtocol","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal0X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal1X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"flash","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"}],"name":"increaseObservationCardinalityNext","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxLiquidityPerTick","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"mint","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"","type":"uint256"}],"name":"observations","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint128","name":"liquidity","type":"uint128"},{"internalType":"uint256","name":"feeGrowthInside0LastX128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthInside1LastX128","type":"uint256"},{"internalType":"uint128","name":"tokensOwed0","type":"uint128"},{"internalType":"uint128","name":"tokensOwed1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFees","outputs":[{"internalType":"uint128","name":"token0","type":"uint128"},{"internalType":"uint128","name":"token1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8","name":"feeProtocol0","type":"uint8"},{"internalType":"uint8","name":"feeProtocol1","type":"uint8"}],"name":"setFeeProtocol","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint16","name":"observationCardinality","type":"uint16"},{"internalType":"uint16","name":"observationCardinalityNext","type":"uint16"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"}],"name":"snapshotCumulativesInside","outputs":[{"internalType":"int56","name":"tickCumulativeInside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityInsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsInside","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bool","name":"zeroForOne","type":"bool"},{"internalType":"int256","name":"amountSpecified","type":"int256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[{"internalType":"int256","name":"amount0","type":"int256"},{"internalType":"int256","name":"amount1","type":"int256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"int16","name":"","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]
    """
)
class UniswapV3LiquiditySnapshot:
    """
    Retrieve and maintain liquidity positions for Uniswap V3 pools.
    """

    def __init__(
        self,
        file: TextIO | str,
        chain_id: int | None = None,
    ):
        file_handle: TextIOWrapper
        json_liquidity_snapshot: Dict[str, Any]

        match file:
            case TextIOWrapper():
                file_handle = file
                json_liquidity_snapshot = ujson.load(file)
            case str():
                with open(file) as file_handle:
                    json_liquidity_snapshot = ujson.load(file_handle)
            case _:  # pragma: no cover
                raise ValueError(f"Unrecognized file type {type(file)}")

        self._chain_id = chain_id if chain_id is not None else config.get_web3().eth.chain_id

        self.newest_block = json_liquidity_snapshot.pop("snapshot_block")

        self._liquidity_snapshot: Dict[ChecksumAddress, Dict[str, Any]] = {
            to_checksum_address(pool_address): {
                "tick_bitmap": {
                    int(k): UniswapV3BitmapAtWord(**v)
                    for k, v in pool_liquidity_snapshot["tick_bitmap"].items()
                },
                "tick_data": {
                    int(k): UniswapV3LiquidityAtTick(**v)
                    for k, v in pool_liquidity_snapshot["tick_data"].items()
                },
            }
            for pool_address, pool_liquidity_snapshot in json_liquidity_snapshot.items()
        }

        logger.info(
            f"Loaded LP snapshot: {len(json_liquidity_snapshot)} pools @ block {self.newest_block}"
        )

        self._liquidity_events: Dict[ChecksumAddress, List[UniswapV3LiquidityEvent]] = dict()

    def _add_pool_if_missing(self, pool_address: ChecksumAddress) -> None:
        try:
            self._liquidity_events[pool_address]
        except KeyError:
            self._liquidity_events[pool_address] = []

        try:
            self._liquidity_snapshot[pool_address]
        except KeyError:
            self._liquidity_snapshot[pool_address] = {}

    def fetch_new_liquidity_events(
        self,
        to_block: int,
        span: int = 1000,
    ) -> None:
        def _process_log() -> Tuple[ChecksumAddress, UniswapV3LiquidityEvent]:
            decoded_event = get_event_data(config.get_web3().codec, event_abi, log)

            pool_address = to_checksum_address(decoded_event["address"])
            tx_index = decoded_event["transactionIndex"]
            liquidity_block = decoded_event["blockNumber"]
            liquidity = decoded_event["args"]["amount"] * (
                -1 if decoded_event["event"] == "Burn" else 1
            )
            tick_lower = decoded_event["args"]["tickLower"]
            tick_upper = decoded_event["args"]["tickUpper"]

            return pool_address, UniswapV3LiquidityEvent(
                block_number=liquidity_block,
                liquidity=liquidity,
                tick_lower=tick_lower,
                tick_upper=tick_upper,
                tx_index=tx_index,
            )

        logger.info(f"Updating snapshot from block {self.newest_block} to {to_block}")

        v3pool = Web3().eth.contract(abi=UNISWAP_V3_POOL_ABI)

        for event in [v3pool.events.Mint, v3pool.events.Burn]:
            logger.info(f"Processing {event.event_name} events")
            event_abi = event._get_event_abi()
            start_block = self.newest_block + 1

            while True:
                end_block = min(to_block, start_block + span - 1)

                _, event_filter_params = construct_event_filter_params(
                    event_abi=event_abi,
                    abi_codec=config.get_web3().codec,
                    fromBlock=start_block,
                    toBlock=end_block,
                )

                event_logs = config.get_web3().eth.get_logs(event_filter_params)

                for log in event_logs:
                    pool_address, liquidity_event = _process_log()

                    if liquidity_event.liquidity == 0:  # pragma: no cover
                        continue

                    self._add_pool_if_missing(pool_address)
                    self._liquidity_events[pool_address].append(liquidity_event)

                if end_block == to_block:
                    break
                else:
                    start_block = end_block + 1

        logger.info(f"Updated snapshot to block {to_block}")
        self.newest_block = to_block

    def get_new_liquidity_updates(self, pool_address: str) -> List[UniswapV3PoolExternalUpdate]:
        pool_address = to_checksum_address(pool_address)
        pool_updates = self._liquidity_events.get(pool_address, list())
        self._liquidity_events[pool_address] = list()

        # The V3LiquidityPool helper will reject liquidity events associated with a past block, so
        # they must be applied in chronological order
        sorted_events = sorted(
            pool_updates,
            key=lambda event: (event.block_number, event.tx_index),
        )

        return [
            UniswapV3PoolExternalUpdate(
                block_number=event.block_number,
                liquidity_change=(
                    event.liquidity,
                    event.tick_lower,
                    event.tick_upper,
                ),
            )
            for event in sorted_events
        ]

    def get_tick_bitmap(self, pool: ChecksumAddress | str) -> Dict[int, UniswapV3BitmapAtWord]:
        pool_address = to_checksum_address(pool)

        try:
            tick_bitmap: Dict[int, UniswapV3BitmapAtWord] = self._liquidity_snapshot[pool_address][
                "tick_bitmap"
            ]
            return tick_bitmap
        except KeyError:
            return dict()

    def get_tick_data(self, pool: ChecksumAddress | str) -> Dict[int, UniswapV3LiquidityAtTick]:
        pool_address = to_checksum_address(pool)

        try:
            tick_data: Dict[int, UniswapV3LiquidityAtTick] = self._liquidity_snapshot[pool_address][
                "tick_data"
            ]
            return tick_data
        except KeyError:
            return {}

    def update_snapshot(
        self,
        pool: ChecksumAddress | str,
        tick_data: Dict[int, UniswapV3LiquidityAtTick],
        tick_bitmap: Dict[int, UniswapV3BitmapAtWord],
    ) -> None:
        pool_address = to_checksum_address(pool)

        self._add_pool_if_missing(pool_address)
        self._liquidity_snapshot[pool_address].update(
            {
                "tick_bitmap": tick_bitmap,
            }
        )
        self._liquidity_snapshot[pool_address].update(
            {
                "tick_data": tick_data,
            }
        )
