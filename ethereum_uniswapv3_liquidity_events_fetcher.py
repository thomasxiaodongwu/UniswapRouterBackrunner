import json
import sys
from threading import Lock
from typing import Dict

import brownie
from web3._utils.events import get_event_data
from web3._utils.filters import construct_event_filter_params
from dto.v3_liquidity_pool import V3LiquidityPool
from dto.abi import *
from dto.v3_liquidity_pool import (
    UniswapV3BitmapAtWord,
    UniswapV3LiquidityAtTick,
    UniswapV3PoolExternalUpdate,
)

BROWNIE_NETWORK = "mainnet-local"
SNAPSHOT_FILENAME = "ethereum_v3_liquidity_snapshot.json"
UNISWAPV3_START_BLOCK = 12_369_621

TICKSPACING_BY_FEE: Dict = {
    100: 1,
    500: 10,
    3000: 60,
    10000: 200,
}


class MockV3LiquidityPool(V3LiquidityPool):
    def __init__(self):
        pass


def prime_pools():
    print("Starting pool primer")

    liquidity_snapshot: Dict[str, Dict] = {}

    lp_data: Dict[str, Dict] = {}

    for path in [
        "ethereum_lps_sushiswapv3.json",
        "ethereum_lps_uniswapv3.json",
    ]:
        try:
            with open(path, "r") as file:
                l = json.load(file)
            for lp in l:
                lp_data[lp["pool_address"]] = lp
        except Exception as e:
            print(e)

    try:
        with open(SNAPSHOT_FILENAME, "r") as file:
            json_liquidity_snapshot = json.load(file)
    except:
        snapshot_last_block = None
    else:
        snapshot_last_block = json_liquidity_snapshot.pop("snapshot_block")
        print(
            f"Loaded LP snapshot: {len(json_liquidity_snapshot)} pools @ block {snapshot_last_block}"
        )

        assert (
            snapshot_last_block < newest_block
        ), f"Aborting, snapshot block ({snapshot_last_block}) is newer than current chain height ({newest_block})"

        # Transform the JSON-encoded info from the snapshot to the dataclass
        # used by V3LiquidityPool
        for pool_address, snapshot in json_liquidity_snapshot.items():
            liquidity_snapshot[pool_address] = {
                "tick_bitmap": {
                    int(k): UniswapV3BitmapAtWord(**v)
                    for k, v in snapshot["tick_bitmap"].items()
                },
                "tick_data": {
                    int(k): UniswapV3LiquidityAtTick(**v)
                    for k, v in snapshot["tick_data"].items()
                },
            }

    V3LP = brownie.web3.eth.contract(
        abi=UNISWAP_V3_POOL_ABI
    )

    liquidity_events = {}

    for event in [V3LP.events.Mint, V3LP.events.Burn]:
        print(f"processing {event.event_name} events")

        start_block = (
            max(UNISWAPV3_START_BLOCK, snapshot_last_block + 1)
            if snapshot_last_block is not None
            else UNISWAPV3_START_BLOCK
        )
        block_span = 10_000
        done = False

        event_abi = event._get_event_abi()

        while not done:
            end_block = min(newest_block, start_block + block_span)

            _, event_filter_params = construct_event_filter_params(
                event_abi=event_abi,
                abi_codec=brownie.web3.codec,
                argument_filters={},
                fromBlock=start_block,
                toBlock=end_block,
            )

            try:
                event_logs = brownie.web3.eth.get_logs(event_filter_params)
            except:
                block_span = int(0.75 * block_span)
                continue

            for event in event_logs:
                decoded_event = get_event_data(
                    brownie.web3.codec, event_abi, event
                )

                pool_address = decoded_event["address"]
                block = decoded_event["blockNumber"]
                tx_index = decoded_event["transactionIndex"]
                liquidity = decoded_event["args"]["amount"] * (
                    -1 if decoded_event["event"] == "Burn" else 1
                )
                tick_lower = decoded_event["args"]["tickLower"]
                tick_upper = decoded_event["args"]["tickUpper"]

                # skip zero liquidity events
                if liquidity == 0:
                    continue

                try:
                    liquidity_events[pool_address]
                except KeyError:
                    liquidity_events[pool_address] = []

                liquidity_events[pool_address].append(
                    (
                        block,
                        tx_index,
                        (
                            liquidity,
                            tick_lower,
                            tick_upper,
                        ),
                    )
                )

            print(f"Fetched events: block span [{start_block},{end_block}]")

            if end_block == newest_block:
                done = True
            else:
                start_block = end_block + 1
                block_span = int(1.05 * block_span)

    lp_helper = MockV3LiquidityPool()
    lp_helper.sparse_bitmap = False
    lp_helper._liquidity_lock = Lock()
    lp_helper._slot0_lock = Lock()
    lp_helper._update_log = list()

    for pool_address in liquidity_events.keys():
        # Ignore all pool addresses not held in the LP data dict. A strange
        # pool (0x820e891b14149e98b48b39ee2667157Ef750539b) was triggering an
        # early termination because it had liquidity events, but was not
        # associated with the known factories.
        if not lp_data.get(pool_address):
            continue

        try:
            previous_snapshot_tick_data = liquidity_snapshot[pool_address][
                "tick_data"
            ]
        except KeyError:
            previous_snapshot_tick_data = {}

        try:
            previous_snapshot_tick_bitmap = liquidity_snapshot[pool_address][
                "tick_bitmap"
            ]
        except KeyError:
            previous_snapshot_tick_bitmap = {}

        lp_helper.address = "0x0000000000000000000000000000000000000000"
        lp_helper.liquidity = 1 << 256
        lp_helper.tick_data = previous_snapshot_tick_data
        lp_helper.tick_bitmap = previous_snapshot_tick_bitmap
        lp_helper.update_block = snapshot_last_block or UNISWAPV3_START_BLOCK
        lp_helper.liquidity_update_block = (
            snapshot_last_block or UNISWAPV3_START_BLOCK
        )
        lp_helper.tick = 0
        lp_helper.fee = lp_data[pool_address]["fee"]
        lp_helper.tick_spacing = TICKSPACING_BY_FEE[lp_helper.fee]

        sorted_liquidity_events = sorted(
            liquidity_events[pool_address],
            key=lambda event: (event[0], event[1]),
        )

        for liquidity_event in sorted_liquidity_events:
            (
                event_block,
                _,
                (liquidity_delta, tick_lower, tick_upper),
            ) = liquidity_event

            # Push the liquidity events into the mock helper
            lp_helper.external_update(
                update=UniswapV3PoolExternalUpdate(
                    block_number=event_block,
                    liquidity_change=(
                        liquidity_delta,
                        tick_lower,
                        tick_upper,
                    ),
                ),
            )

        # After all events have been pushed, update the liquidity snapshot with
        # the full liquidity data from the helper
        try:
            liquidity_snapshot[pool_address]
        except KeyError:
            liquidity_snapshot[pool_address] = {
                "tick_bitmap": {},
                "tick_data": {},
            }

        liquidity_snapshot[pool_address]["tick_bitmap"].update(
            lp_helper.tick_bitmap
        )
        liquidity_snapshot[pool_address]["tick_data"].update(
            lp_helper.tick_data
        )

    for pool_address in liquidity_snapshot:
        # Convert all liquidity data to JSON format so it can be exported
        liquidity_snapshot[pool_address] = {
            "tick_data": {
                key: value.to_dict()
                for key, value in liquidity_snapshot[pool_address][
                    "tick_data"
                ].items()
            },
            "tick_bitmap": {
                key: value.to_dict()
                for key, value in liquidity_snapshot[pool_address][
                    "tick_bitmap"
                ].items()
                if value.bitmap  # skip empty bitmaps
            },
        }

    liquidity_snapshot["snapshot_block"] = newest_block

    with open(SNAPSHOT_FILENAME, "w") as file:
        json.dump(
            liquidity_snapshot,
            file,
            indent=2,
            sort_keys=True,
        )
        print("Wrote LP snapshot")


try:
    brownie.network.connect(BROWNIE_NETWORK)
except:
    sys.exit(
        "Could not connect! Verify your Brownie network settings using 'brownie networks list'"
    )

newest_block = brownie.chain.height

if __name__ == "__main__":
    prime_pools()
    print("Complete")