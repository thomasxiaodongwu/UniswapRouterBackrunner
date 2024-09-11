import asyncio
import dataclasses
import json
import logging
import multiprocessing
import os
import signal
import sys
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Union

import aiohttp
import brownie  # type: ignore
import eth_abi
import eth_account
import web3
import websockets.client
from eth_typing import ChecksumAddress
from hexbytes import HexBytes

from dto.arbitrage_dataclasses import ArbitrageCalculationResult
from dto.uniswap_lp_cycle import UniswapLpCycle
from dto.exceptions import *
from dto.v3_snapshot import (
    UniswapV3LiquiditySnapshot,
    UniswapV3PoolExternalUpdate,
)
from dto.managers import (
    UniswapV2LiquidityPoolManager,
    UniswapV3LiquidityPoolManager,
)
from dto.v2_liquidity_pool import CamelotLiquidityPool, LiquidityPool
from dto.pool_manager import AllPools
from dto.v3_liquidity_pool import V3LiquidityPool
from dto.erc20_token import Erc20Token
from dto.uniswap_transaction import UniswapTransaction

BROWNIE_NETWORK = "mainnet-local"
BROWNIE_ACCOUNT = "mainnet_bot"
FLASHBOTS_IDENTITY_ACCOUNT = "flashbots_id"
EXECUTOR_CONTRACT_ADDRESS = "EDITME"
EXECUTOR_CONTRACT_ABI = """
    [{"stateMutability": "payable", "type": "constructor", "inputs": [], "outputs": []}, {"stateMutability": "payable", "type": "function", "name": "execute_payloads", "inputs": [{"name": "payloads", "type": "tuple[]", "components": [{"name": "target", "type": "address"}, {"name": "calldata", "type": "bytes"}, {"name": "value", "type": "uint256"}]}], "outputs": []}, {"stateMutability": "payable", "type": "function", "name": "execute_payloads", "inputs": [{"name": "payloads", "type": "tuple[]", "components": [{"name": "target", "type": "address"}, {"name": "calldata", "type": "bytes"}, {"name": "value", "type": "uint256"}]}, {"name": "balance_check", "type": "bool"}], "outputs": []}, {"stateMutability": "payable", "type": "function", "name": "uniswapV3SwapCallback", "inputs": [{"name": "amount0_delta", "type": "int256"}, {"name": "amount1_delta", "type": "int256"}, {"name": "data", "type": "bytes"}], "outputs": []}, {"stateMutability": "payable", "type": "fallback"}]
    """
NODE_HTTP_URI = "http://localhost:8545"
NODE_WEBSOCKET_URI = "ws://localhost:8546"
ETHERSCAN_API_KEY = "EDITME"
WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"
MIN_PROFIT_GROSS = int(
    0.001 * 10**18
)  # first filter for gross arb profit (excludes gas)
MIN_PROFIT_NET = int(
    0.0001 * 10**18
)  # second filter for net arb profit (includes gas)
SUBSCRIBE_TO_FULL_TRANSACTIONS = False

BUILDER_BRIBE = 0.95  # % of profit to bribe the builder (via relay)
FLASHBOTS_RELAY_URL = "https://relay.flashbots.net"
RELAY_RETRIES = 3
RELAYS = [
    FLASHBOTS_RELAY_URL,
    "https://builder0x69.io",
]

VERBOSE_BLOCKS = True
VERBOSE_PROCESSING = False
VERBOSE_RELAY_SIMULATION = True
VERBOSE_TIMING = False
VERBOSE_UPDATES = False
VERBOSE_WATCHDOG = True
LATE_BLOCK_THRESHOLD = 5.0
REDUCE_TRIANGLE_ARBS = True
DRY_RUN = True


@dataclasses.dataclass
class BotStatus:
    last_base_fee: int
    next_base_fee: int
    newest_block: int
    newest_block_timestamp: int
    chain_id: int
    live: bool = False
    first_block: int = 0
    first_event: int = 0
    watching_blocks: bool = False
    watching_events: bool = False
    weth_balance: int = 10 * 10**18
    average_blocktime: float = 12.0
    pool_managers_ready: bool = False
    paused: bool = True


def _handle_task_exception(*_):
    """
    By default, do nothing
    """


def _pool_worker_init():
    """
    Ignore SIGINT signals. Used for subprocesses spawned by `ProcessPoolExecutor` via the `initializer=` argument.

    Otherwise SIGINT is translated to KeyboardInterrupt, which is unhandled and will lead to messy tracebacks when thrown into a subprocess.
    """
    signal.signal(signal.SIGINT, signal.SIG_IGN)


async def main() -> None:
    bot_status = BotStatus(
        chain_id=brownie.chain.id,
        last_base_fee=1 * 10**9,
        next_base_fee=1 * 10**9,
        newest_block=brownie.chain.height,
        newest_block_timestamp=int(time.time()),
    )
    all_arbs: Dict[str, UniswapLpCycle] = dict()
    all_pools = AllPools(bot_status.chain_id)
    all_tasks: Set[asyncio.Task] = set()
    pending_tx: Set[str] = set()
    pool_managers: Dict[
        str,
        Union[
            UniswapV2LiquidityPoolManager,
            UniswapV3LiquidityPoolManager,
        ],
    ] = dict()

    snapshot = UniswapV3LiquiditySnapshot(
        file="ethereum_v3_liquidity_snapshot.json"
    )

    asyncio.get_running_loop().set_exception_handler(_handle_task_exception)

    signals = (
        signal.SIGHUP,
        signal.SIGTERM,
        signal.SIGINT,
        # signal.SIGBREAK, # For Windows users, will catch CTRL+C
    )

    for sig in signals:
        asyncio.get_event_loop().add_signal_handler(
            sig,
            shutdown,
            all_tasks,
        )

    with ProcessPoolExecutor(
        # max_workers=16,
        mp_context=multiprocessing.get_context("spawn"),
        initializer=_pool_worker_init,
    ) as process_pool:
        async with aiohttp.ClientSession(
            raise_for_status=True
        ) as http_session:
            for coro in [
                load_arbs(
                    all_arbs=all_arbs,
                    all_pools=all_pools,
                    bot_status=bot_status,
                    snapshot=snapshot,
                    pool_managers=pool_managers,
                ),
                track_balance(
                    all_arbs=all_arbs,
                    bot_status=bot_status,
                ),
                watch_events(
                    bot_status=bot_status,
                    all_pools=all_pools,
                    snapshot=snapshot,
                    pool_managers=pool_managers,
                ),
                watch_new_blocks(
                    bot_status=bot_status,
                ),
                watch_pending_transactions(
                    http_session=http_session,
                    pending_tx=pending_tx,
                    all_arbs=all_arbs,
                    bot_status=bot_status,
                    process_pool=process_pool,
                ),
                watchdog(
                    bot_status=bot_status,
                ),
            ]:
                task = asyncio.create_task(coro)
                task.add_done_callback(all_tasks.discard)
                all_tasks.add(task)

            try:
                await asyncio.gather(*all_tasks)
            except asyncio.CancelledError:
                return
            except:
                logger.exception("(main) catch-all")


async def execute_arb_with_relay(
    http_session: aiohttp.ClientSession,
    arb_result: ArbitrageCalculationResult,
    bot_status: BotStatus,
    all_arbs: dict,
    state_block: int,
    target_block: int,
    tx_to_backrun: Optional[HexBytes] = None,
    tx_to_frontrun: Optional[HexBytes] = None,
):
    """
    Generate, simulate, check profitability, and send a bundle to one or more
    Flashbots-compatible relays.
    """

    class SimulationError(Exception):
        pass

    class RelayError(Exception):
        pass

    async def simulate_bundle(bundle, state_block, target_block) -> dict:
        """
        Simulate the bundle via the HTTP endpoint.

        Ref: https://docs.flashbots.net/flashbots-auction/searchers/advanced/rpc-endpoint#eth_callbundle
        """

        simulation_payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_callBundle",
                "params": [
                    {
                        "txs": bundle,
                        "blockNumber": hex(target_block),
                        "stateBlockNumber": hex(state_block),
                    }
                ],
            }
        )

        simulation_message = eth_account.Account.sign_message(
            eth_account.messages.encode_defunct(
                text=web3.Web3.keccak(text=simulation_payload).hex()
            ),
            flashbots_id_account.private_key,
        )
        simulation_signature = (
            flashbots_id_account.address
            + ":"
            + simulation_message.signature.hex()
        )
        simulation_headers = {
            "Content-Type": "application/json",
            "X-Flashbots-Signature": simulation_signature,
        }

        try:
            async with http_session.post(
                url=FLASHBOTS_RELAY_URL,
                headers=simulation_headers,
                data=simulation_payload,
            ) as resp:
                relay_response = await resp.json()
                # SAMPLE RESPONSE
                # {
                #     "id": 1,
                #     "jsonrpc": "2.0",
                #     "result": {
                #         "results": [
                #             {
                #                 "txHash": "0x70adb851ffd1649932a37d96ddd0d7ab08874df636de1d75e800f48c204d62e0",
                #                 "gasUsed": 186151,
                #                 "gasPrice": "100000000",
                #                 "gasFees": "18615100000000",
                #                 "fromAddress": "0x...",
                #                 "toAddress": "0x...",
                #                 "coinbaseDiff": "18615100000000",
                #                 "ethSentToCoinbase": "0",
                #                 "value": "0x",
                #             },
                #             {
                #                 "txHash": "0xe97aa90bb00e61f68a165d77eb9346398017ae4a4bc580993a8ac09dcb2417d5",
                #                 "gasUsed": 190841,
                #                 "gasPrice": "0",
                #                 "gasFees": "0",
                #                 "fromAddress": "0x...",
                #                 "toAddress": "0x...",
                #                 "coinbaseDiff": "0",
                #                 "ethSentToCoinbase": "0",
                #                 "error": "execution reverted",
                #                 "revert": "\x08�y�\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0cUniswapV2: K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                #                 "value": None,
                #             },
                #         ],
                #         "coinbaseDiff": "18615100000000",
                #         "gasFees": "18615100000000",
                #         "ethSentToCoinbase": "0",
                #         "bundleGasPrice": "49377970",
                #         "totalGasUsed": 376992,
                #         "stateBlockNumber": 17893330,
                #         "bundleHash": "0x...",
                #     },
                # }
        except aiohttp.ClientResponseError as e:
            print(f"(eth_callBundle) {type(e)}: {e}")
            raise SimulationError from e
        else:
            result = relay_response.get("result")
            if result:
                return result
            else:
                raise SimulationError(f"{relay_response=}")

    async def send_bundle(
        bundle: List[str], relay_address, target_block, retries
    ) -> dict:
        """
        Send the bundle via the HTTP endpoint.

        Ref: https://docs.flashbots.net/flashbots-auction/searchers/advanced/rpc-endpoint#eth_sendbundle
        """

        send_bundle_payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_sendBundle",
                "params": [
                    {
                        "txs": bundle,
                        "blockNumber": hex(target_block),
                    }
                ],
            }
        )

        send_bundle_message = eth_account.Account.sign_message(
            eth_account.messages.encode_defunct(
                text=web3.Web3.keccak(text=send_bundle_payload).hex()
            ),
            flashbots_id_account.private_key,
        )
        send_bundle_signature = (
            flashbots_id_account.address
            + ":"
            + send_bundle_message.signature.hex()
        )
        send_bundle_headers = {
            "Content-Type": "application/json",
            "X-Flashbots-Signature": send_bundle_signature,
            "X-Auction-Signature": send_bundle_signature,
        }

        for _ in range(retries):
            try:
                async with http_session.post(
                    url=relay_address,
                    headers=send_bundle_headers,
                    data=send_bundle_payload,
                ) as resp:
                    relay_response = await resp.json(
                        content_type=None,  # some relays do not correctly set the MIME type for JSON data
                    )
            except aiohttp.ClientResponseError as e:
                raise RelayError(f"(send_bundle) HTTP Error: {e}") from e
            else:
                return relay_response["result"]

        raise RelayError("Retries exceeded")

    def build_transaction(payloads, tx_params):
        """
        Build a transaction for the `execute_payloads` function.
        """
        return (
            w3.eth.contract(address=arb_contract.address, abi=arb_contract.abi)
            .functions.execute_payloads(payloads)
            .buildTransaction(tx_params)
        )

    def sign_transaction(tx) -> HexBytes:
        """
        Sign the transaction dictionary using a private key.
        """
        return (
            eth_account.Account.from_key(bot_account.private_key)
            .sign_transaction(tx)
            .rawTransaction
        )

    coro_start = time.perf_counter()

    if target_block <= bot_status.newest_block:
        return

    # Get the arb helper by its ID
    arb_helper = all_arbs.get(arb_result.id)

    if arb_helper is None:
        return

    if TYPE_CHECKING:
        assert isinstance(arb_helper, UniswapLpCycle)

    tx_params = {
        "from": bot_account.address,
        "chainId": brownie.chain.id,
        "gas": int(1.25 * arb_helper.gas_estimate)
        if arb_helper.gas_estimate
        else 250_000,
        "nonce": bot_account.nonce,
        "maxFeePerGas": int(1.25 * bot_status.next_base_fee),
        "maxPriorityFeePerGas": 0,
        "value": 0,
        "accessList": getattr(arb_helper, "access_list", list()),
    }

    arb_payloads = arb_helper.generate_payloads(
        from_address=arb_contract.address,
        swap_amount=arb_result.input_amount,
        pool_swap_amounts=arb_result.swap_amounts,
    )

    arbitrage_transaction = build_transaction(arb_payloads, tx_params)
    arbitrage_transaction_raw = sign_transaction(arbitrage_transaction).hex()

    bundled_tx: List[str] = []
    if tx_to_backrun is not None:
        bundled_tx.append(tx_to_backrun.hex())
    bundled_tx.append(arbitrage_transaction_raw)
    if tx_to_frontrun is not None:
        bundled_tx.append(tx_to_frontrun.hex())

    arbitrage_transaction_bundle_position = bundled_tx.index(
        arbitrage_transaction_raw
    )

    if not hasattr(arb_helper, "access_list"):
        try:
            async with http_session.post(
                url=NODE_HTTP_URI,
                headers={"content-type": "application/json"},
                data=json.dumps(
                    {
                        "method": "eth_createAccessList",
                        "id": 1,
                        "jsonrpc": "2.0",
                        "params": [
                            {
                                "from": arbitrage_transaction["from"],
                                "to": arbitrage_transaction["to"],
                                "data": arbitrage_transaction["data"],
                                "gas": hex(arbitrage_transaction["gas"]),
                            },
                        ],
                    }
                ),
            ) as resp:
                gas_used_with_access_list: Optional[int] = None
                result = await resp.json()
                if result.get("error"):
                    logger.info("Error generating access list!")
                    logger.info(result)
                else:
                    access_list = result["result"]["accessList"]
                    gas_used_with_access_list = int(
                        result["result"]["gasUsed"],
                        16,
                    )
                    logger.info(
                        f"Generated access list ({gas_used_with_access_list} gas)"
                    )
        except Exception as e:
            print(e)
        else:
            # Include the access list and update the estimate if the access
            # list provides gas savings compared to the "vanilla" TX
            if (
                gas_used_with_access_list is not None
                and gas_used_with_access_list < arb_helper.gas_estimate
            ):
                logger.info(
                    f"ADDED ACCESS LIST! (reduced {arb_helper.gas_estimate} to {gas_used_with_access_list})"
                )
                # Add the access list to the arb helper (does not need to be
                # regenerated)
                setattr(arb_helper, "access_list", access_list)
                # Add the access list to the transaction parameters
                tx_params.update(
                    {
                        "accessList": access_list,
                    }
                )
                # Regenerate the transaction with the new access list
                arbitrage_transaction = build_transaction(
                    arb_payloads, tx_params
                )
                arbitrage_transaction_raw = sign_transaction(
                    arbitrage_transaction
                ).hex()

    # Simulate the whole bundle if it includes a frontrun/backrun
    if tx_to_frontrun or tx_to_backrun:
        attempts = 0
        while True:
            if attempts == RELAY_RETRIES:
                return

            attempts += 1

            try:
                simulation = await simulate_bundle(
                    bundled_tx,
                    state_block=state_block,
                    target_block=target_block,
                )
            except Exception as e:
                print(e)
            else:
                sim_error = False
                for result in simulation["results"]:
                    if result.get("error"):
                        sim_error = True
                        if VERBOSE_RELAY_SIMULATION:
                            logger.info(f'Reverted: {result.get("revert")}')
                if sim_error:
                    return
                else:
                    break

            # SAMPLE RESULT:
            # {
            #     "id": 1,
            #     "jsonrpc": "2.0",
            #     "result": {
            #         "results": [
            #             {
            #                 "txHash": "0x5387010c3acf80c94e86af6a401b626be6d8b8abc9f0ae51d268986d3c560f4f",
            #                 "gasUsed": 165422,
            #                 "gasPrice": "100000000",
            #                 "gasFees": "16542200000000",
            #                 "fromAddress": "0x...",
            #                 "toAddress": "0x...",
            #                 "coinbaseDiff": "16542200000000",
            #                 "ethSentToCoinbase": "0",
            #                 "value": "0x",
            #             },
            #             {
            #                 "txHash": "0x9001a7af23c8f858b9e549efc80d26229c26977422a3066e66ef5a253cf700d9",
            #                 "gasUsed": 191955,
            #                 "gasPrice": "0",
            #                 "gasFees": "0",
            #                 "fromAddress": "0x...",
            #                 "toAddress": "0x...",
            #                 "coinbaseDiff": "0",
            #                 "ethSentToCoinbase": "0",
            #                 "error": "execution reverted",
            #                 "revert": "\x08�y�\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00 \x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x0cUniswapV2: K\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",
            #                 "value": None,
            #             },
            #         ],
            #         "coinbaseDiff": "16542200000000",
            #         "gasFees": "16542200000000",
            #         "ethSentToCoinbase": "0",
            #         "bundleGasPrice": "46287813",
            #         "totalGasUsed": 357377,
            #         "stateBlockNumber": 17887845,
            #         "bundleHash": "0x...",
            #     },
            # }

        simulated_gas_use = sum(
            [
                tx["gasUsed"]
                for tx in simulation["results"]
                if tx["fromAddress"] == bot_account.address
            ]
        )

        arb_helper.gas_estimate = simulated_gas_use

    gas_fee = arb_helper.gas_estimate * bot_status.next_base_fee
    arb_net_profit = arb_result.profit_amount - gas_fee

    logger.info(f"Arb     : {arb_helper}")
    logger.info(
        f"Profit  : {arb_result.profit_amount/(10**arb_result.profit_token.decimals):0.5f} ETH"
    )
    logger.info(
        f"Gas Fee : {gas_fee/(10**18):0.5f} ETH ({arb_helper.gas_estimate} gas estimate)"
    )
    logger.info(f"Net     : {arb_net_profit/(10**18):0.5f} ETH")

    if arb_net_profit >= MIN_PROFIT_NET:
        if tx_to_backrun:
            logger.info("*** EXECUTING BACKRUN ARB (RELAY) ***")
        elif tx_to_frontrun:
            logger.info("*** EXECUTING FRONTRUN ARB (RELAY) ***")
        else:
            logger.info("*** EXECUTING ONCHAIN ARB (RELAY) ***")

        bribe = max(0, int(BUILDER_BRIBE * arb_net_profit))
        bribe_gas = bribe // arb_helper.gas_estimate
        logger.info(f"BRIBE SET TO {bribe/(10**18):0.5f} ETH")

        # Rebuild the arbitrage transaction with the new gas values
        tx_params.update(
            {
                "maxFeePerGas": bot_status.next_base_fee + bribe_gas,
                "maxPriorityFeePerGas": bribe_gas,
            }
        )
        arbitrage_transaction = build_transaction(arb_payloads, tx_params)
        arbitrage_transaction_raw = sign_transaction(
            arbitrage_transaction
        ).hex()

        # Replace the arbitrage transaction in the original bundle
        bundled_tx[
            arbitrage_transaction_bundle_position
        ] = arbitrage_transaction_raw

        logger.info("Bundle rebuilt")
        logger.info("Simulating final bundle")

        # simulate with the final gas values before submitting
        attempts = 0
        while True:
            if attempts == RELAY_RETRIES:
                return False

            attempts += 1

            try:
                simulation = await simulate_bundle(
                    bundled_tx,
                    state_block=state_block,
                    target_block=target_block,
                )
            except Exception as e:
                print(e)
            else:
                sim_error = False
                for result in simulation["results"]:
                    if result.get("error"):
                        sim_error = True
                        if VERBOSE_RELAY_SIMULATION:
                            logger.info(f'Reverted: {result.get("revert")}')

                if sim_error:
                    return
                else:
                    break  # break out of while loop

        simulated_gas_use = sum(
            [
                tx["gasUsed"]
                for tx in simulation["results"]
                if tx["fromAddress"] == bot_account.address
            ]
        )

        logger.info(f'Simulation            : {simulation["results"]}')
        logger.info(f"Simulation Gas        : {simulated_gas_use}")
        logger.info(f"Simulation bundleHash : {simulation['bundleHash']}")

        if simulated_gas_use > arb_helper.gas_estimate:
            logger.info(
                f"ABORTING! SIMULATED GAS USE ({simulated_gas_use}) EXCEEDS ESTIMATE ({arb_helper.gas_estimate})"
            )
            return

        if tx_to_backrun or tx_to_frontrun:
            bundle_valid_blocks = 3
        else:
            bundle_valid_blocks = 1

        if DRY_RUN:
            logger.info("RELAY SUBMISSION CANCELLED (DRY RUN ACTIVE)")
            return False

        submitted_blocks = set()
        tasks = [
            send_bundle(
                bundle=bundled_tx,
                relay_address=relay,
                target_block=target_block,
                retries=RELAY_RETRIES,
            )
            for target_block in range(
                target_block,
                target_block + bundle_valid_blocks,
            )
            for relay in RELAYS
        ]

        for i in range(bundle_valid_blocks):
            logger.info(f"Sending bundle targeting block {target_block +  i}")
            submitted_blocks.add(target_block + i)

        logger.info("All bundles sent")

        for coro in asyncio.as_completed(tasks):
            try:
                await coro
            except Exception as e:
                print(f"(send_bundle):{e}")

        record_bundle(
            bundle_hash=simulation["bundleHash"],
            blocks=list(submitted_blocks),
            tx=bundled_tx,
            arb_id=arb_helper.id,
        )
        logger.info("Bundle recorded!")

        return True

    if VERBOSE_TIMING:
        logger.info(
            f"send_arb_via_relay completed in {time.perf_counter() - coro_start:0.4f}s"
        )


async def load_arbs(
    all_pools: AllPools,
    all_arbs: Dict[str, UniswapLpCycle],
    bot_status: BotStatus,
    snapshot: UniswapV3LiquiditySnapshot,
    pool_managers: Dict[
        str,
        Union[
            UniswapV2LiquidityPoolManager,
            UniswapV3LiquidityPoolManager,
        ],
    ],
):
    """
    Process all known liquidity pools and arbitrage paths from JSON to `UniswapLpCycle` helpers
    """

    logger.info("Starting arb loading function")

    # TODO: figure out timing
    while not bot_status.first_event:
        await asyncio.sleep(0)

    # update to the block BEFORE the event watcher came online
    snapshot.fetch_new_liquidity_events(bot_status.first_event - 1)

    # Uniswap V2 UniswapV2Factory
    univ2_lp_manager = UniswapV2LiquidityPoolManager(
        factory_address="0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    )
    pool_managers[
        "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f"
    ] = univ2_lp_manager

    # Uniswap V3 UniswapV3Factory
    univ3_lp_manager = UniswapV3LiquidityPoolManager(
        factory_address="0x1F98431c8aD98523631AE4a59f267346ea31F984",
        snapshot=snapshot,
    )
    pool_managers[
        "0x1F98431c8aD98523631AE4a59f267346ea31F984"
    ] = univ3_lp_manager

    # Sushiswap V2
    sushiv2_lp_manager = UniswapV2LiquidityPoolManager(
        factory_address="0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac"
    )
    pool_managers[
        "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac"
    ] = sushiv2_lp_manager

    # Sushiswap V3
    sushiv3_lp_manager = UniswapV3LiquidityPoolManager(
        factory_address="0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F",
        snapshot=snapshot,
    )
    pool_managers[
        "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F"
    ] = sushiv3_lp_manager

    bot_status.pool_managers_ready = True

    liquidity_pool_data = {}
    for liquidity_pool_filename in [
        "ethereum_lps_sushiswapv2.json",
        "ethereum_lps_sushiswapv3.json",
        "ethereum_lps_uniswapv2.json",
        "ethereum_lps_uniswapv3.json",
    ]:
        with open(liquidity_pool_filename, encoding="utf-8") as file:
            for pool in json.load(file):
                if (pool_address := pool["pool_address"]) in BLACKLISTED_POOLS:
                    continue
                if pool["token0"] in BLACKLISTED_TOKENS:
                    continue
                if pool["token1"] in BLACKLISTED_TOKENS:
                    continue
                liquidity_pool_data[pool_address] = pool
    logger.info(f"Found {len(liquidity_pool_data)} pools")

    arb_paths = []
    for arb_filename in [
        "ethereum_arbs_2pool.json",
        # "ethereum_arbs_3pool.json",
    ]:
        with open(arb_filename, encoding="utf-8") as file:
            for arb_id, arb in json.load(file).items():
                passed_checks = True
                if arb_id in BLACKLISTED_ARBS:
                    passed_checks = False
                for pool_address in arb["path"]:
                    if not liquidity_pool_data.get(pool_address):
                        passed_checks = False
                if passed_checks:
                    arb_paths.append(arb)
    logger.info(f"Found {len(arb_paths)} arb paths")

    # Identify all unique pool addresses in arb paths
    unique_pool_addresses = {
        pool_address
        for arb in arb_paths
        for pool_address in arb["path"]
        if liquidity_pool_data.get(pool_address)
    }
    logger.info(f"Found {len(unique_pool_addresses)} unique pools")

    start = time.perf_counter()

    while not bot_status.first_event:
        await asyncio.sleep(bot_status.average_blocktime)

    for pool_address in unique_pool_addresses:
        await asyncio.sleep(0)

        pool_type: str = liquidity_pool_data[pool_address]["type"]

        pool_helper: Optional[
            Union[
                LiquidityPool,
                V3LiquidityPool,
            ]
        ] = None

        if pool_type == "UniswapV2":
            try:
                pool_helper = univ2_lp_manager.get_pool(
                    pool_address=pool_address,
                    silent=True,
                    update_method="external",
                    # state_block=bot_status.first_event - 1,
                )
            except ManagerError as exc:
                print(exc)
                continue

        elif pool_type == "SushiswapV2":
            try:
                pool_helper = sushiv2_lp_manager.get_pool(
                    pool_address=pool_address,
                    silent=True,
                    update_method="external",
                    # state_block=bot_status.first_event - 1,
                )
            except ManagerError as exc:
                print(exc)
                continue

        elif pool_type == "UniswapV3":
            try:
                pool_helper = univ3_lp_manager.get_pool(
                    pool_address=pool_address,
                    silent=True,
                    # state_block=bot_status.first_event - 1,
                    v3liquiditypool_kwargs={
                        "fee": liquidity_pool_data[pool_address]["fee"]
                    },
                )
            except ManagerError as exc:
                print(exc)
                continue

        elif pool_type == "SushiswapV3":
            try:
                pool_helper = sushiv3_lp_manager.get_pool(
                    pool_address=pool_address,
                    silent=True,
                    # state_block=bot_status.first_event - 1,
                    v3liquiditypool_kwargs={
                        "fee": liquidity_pool_data[pool_address]["fee"]
                    },
                )
            except ManagerError as exc:
                print(exc)
                continue

        else:
            raise Exception(f"Could not identify pool type! {pool_type=}")

        if isinstance(pool_helper, V3LiquidityPool):
            assert pool_helper.sparse_bitmap == False

        if TYPE_CHECKING:
            assert isinstance(
                pool_helper,
                (
                    LiquidityPool,
                    V3LiquidityPool,
                ),
            )

    logger.info(
        f"Created {len(all_pools)} liquidity pool helpers in {time.perf_counter() - start:.2f}s"
    )

    degenbot_erc20token_weth = Erc20Token(WETH_ADDRESS)

    for arb in arb_paths:
        await asyncio.sleep(0)
        # ignore arbs on the blacklist
        if (arb_id := arb.get("id")) in BLACKLISTED_ARBS:
            continue

        # ignore arbs where pool helpers are not available for all pools in the path
        if len(
            swap_pools := [
                pool_obj
                for pool_address in arb["path"]
                if (pool_obj := all_pools.get(pool_address))
            ]
        ) != len(arb["path"]):
            continue

        all_arbs[arb_id] = UniswapLpCycle(
            input_token=degenbot_erc20token_weth,
            swap_pools=swap_pools,
            max_input=bot_status.weth_balance,
            id=arb_id,
        )

    logger.info(f"Built {len(all_arbs)} cycle arb helpers")
    logger.info("Arb loading complete")
    bot_status.live = True


def record_bundle(
    bundle_hash: str,
    blocks: List[int],
    tx: List[str],
    arb_id: str,
):
    bundle_entry = {
        "blocks": blocks,
        "transactions": tx,
        "arb_id": arb_id,
        "time": time.time(),
    }

    SUBMITTED_BUNDLES[bundle_hash] = bundle_entry

    with open("submitted_bundles.json", "w") as file:
        json.dump(SUBMITTED_BUNDLES, file, indent=2)


def shutdown(tasks: Set[asyncio.Task]):
    """
    Cancel all tasks in the `tasks` set
    """

    logger.info(f"\nCancelling tasks")
    for task in [t for t in tasks if not (t.done() or t.cancelled())]:
        task.cancel()


async def track_balance(
    all_arbs: Dict,
    bot_status: BotStatus,
):
    weth = Erc20Token(WETH_ADDRESS)

    while True:
        await asyncio.sleep(bot_status.average_blocktime)

        try:
            balance = weth._brownie_contract.balanceOf(arb_contract.address)
        except asyncio.exceptions.CancelledError:
            return
        except:
            logger.exception("(track_balance)")
        else:
            if bot_status.weth_balance != balance:
                bot_status.weth_balance = balance
                for arb in all_arbs.values():
                    arb.max_input = balance
                logger.info(f"Updated balance: {balance/(10**18):.3f} WETH")


async def watchdog(bot_status: BotStatus):
    """
    Tasked with monitoring other coroutines, functions, objects, etc. and
    setting bot status variables like `status.paused`

    Other coroutines should monitor the state of `block_status.paused` and adjust their activity as needed
    """

    logger.info("Starting status watchdog")

    while True:
        try:
            await asyncio.sleep(bot_status.average_blocktime)

            # our node will always be slightly delayed compared to the timestamp of the block,
            # so compare that difference on each pass through the loop
            if (
                late_timer := (time.time() - bot_status.newest_block_timestamp)
            ) > bot_status.average_blocktime + LATE_BLOCK_THRESHOLD:
                # if the expected block is late, set the paused flag to True
                if not bot_status.paused:
                    bot_status.paused = True
                    if VERBOSE_WATCHDOG:
                        logger.info(
                            f"WATCHDOG: paused (block {late_timer:.1f}s late)"
                        )
            else:
                if bot_status.paused:
                    bot_status.paused = False
                    if VERBOSE_WATCHDOG:
                        logger.info("WATCHDOG: unpaused")

        except asyncio.exceptions.CancelledError:
            return


async def watch_events(
    bot_status: BotStatus,
    all_pools: AllPools,
    snapshot: UniswapV3LiquiditySnapshot,
    pool_managers: Dict[
        str,
        Union[
            UniswapV2LiquidityPoolManager,
            UniswapV3LiquidityPoolManager,
        ],
    ],
):
    event_queue: deque = deque()

    logger.info("Starting event watcher loop")

    def process_burn_event(message: dict):
        event_address = w3.toChecksumAddress(
            message["params"]["result"]["address"]
        )
        event_block = int(
            message["params"]["result"]["blockNumber"],
            16,
        )
        event_data = message["params"]["result"]["data"]

        v3_pool_helper: Optional[
            Union[V3LiquidityPool, LiquidityPool]
        ] = None
        for pool_manager in pool_managers.values():
            try:
                v3_pool_helper = pool_manager.get_pool(
                    pool_address=event_address,
                    silent=True,
                    # WIP: use previous block state to avoid double-counting liquidity events
                    state_block=event_block - 1,
                )
            except ManagerError:
                continue
            else:
                break

        if v3_pool_helper is None:
            # ignore events for unknown pools
            return

        if TYPE_CHECKING:
            assert isinstance(v3_pool_helper, V3LiquidityPool)

        try:
            _, _, lower, upper = message["params"]["result"]["topics"]
            event_tick_lower = eth_abi.decode_single(
                "int24", bytes.fromhex(lower[2:])
            )
            event_tick_upper = eth_abi.decode_single(
                "int24", bytes.fromhex(upper[2:])
            )
            event_liquidity, _, _ = eth_abi.decode(
                ["uint128", "uint256", "uint256"],
                bytes.fromhex(event_data[2:]),
            )
        except KeyError:
            return
        else:
            if event_liquidity == 0:
                return

            event_liquidity *= -1

            try:
                v3_pool_helper.external_update(
                    update=UniswapV3PoolExternalUpdate(
                        block_number=event_block,
                        liquidity_change=(
                            event_liquidity,
                            event_tick_lower,
                            event_tick_upper,
                        ),
                    ),
                )
                snapshot.update_snapshot(
                    pool=event_address,
                    tick_bitmap=v3_pool_helper.tick_bitmap,
                    tick_data=v3_pool_helper.tick_data,
                )
            # WIP: sys.exit to kill the bot on a failed assert
            # looking to fix "assert self.liquidity >= 0" throwing on some Burn events
            except AssertionError:
                logger.exception(
                    f"(process_burn_event) AssertionError: {message}"
                )
                logger.info(f"{v3_pool_helper._update_log=}")
                sys.exit()
            except:
                logger.exception(f"(process_burn_event): {message}")

    def process_mint_event(message: dict):
        event_address = w3.toChecksumAddress(
            message["params"]["result"]["address"]
        )
        event_block = int(
            message["params"]["result"]["blockNumber"],
            16,
        )
        event_data = message["params"]["result"]["data"]

        v3_pool_helper: Optional[
            Union[V3LiquidityPool, LiquidityPool]
        ] = None
        for pool_manager in pool_managers.values():
            try:
                v3_pool_helper = pool_manager.get_pool(
                    pool_address=event_address,
                    silent=True,
                    # WIP: use previous block state to avoid double-counting liquidity events
                    state_block=event_block - 1,
                )
            except ManagerError:
                continue
            else:
                break

        if v3_pool_helper is None:
            # ignore events for unknown pools
            return

        if TYPE_CHECKING:
            assert isinstance(v3_pool_helper, V3LiquidityPool)

        try:
            _, _, lower, upper = message["params"]["result"]["topics"]
            event_tick_lower = eth_abi.decode_single(
                "int24", bytes.fromhex(lower[2:])
            )
            event_tick_upper = eth_abi.decode_single(
                "int24", bytes.fromhex(upper[2:])
            )
            _, event_liquidity, _, _ = eth_abi.decode(
                ["address", "uint128", "uint256", "uint256"],
                bytes.fromhex(event_data[2:]),
            )
        except KeyError:
            return
        else:
            if event_liquidity == 0:
                return

            try:
                v3_pool_helper.external_update(
                    update=UniswapV3PoolExternalUpdate(
                        block_number=event_block,
                        liquidity_change=(
                            event_liquidity,
                            event_tick_lower,
                            event_tick_upper,
                        ),
                    ),
                )
                snapshot.update_snapshot(
                    pool=event_address,
                    tick_bitmap=v3_pool_helper.tick_bitmap,
                    tick_data=v3_pool_helper.tick_data,
                )

            except Exception as exc:
                logger.exception(f"(process_mint_event): {exc}")

    def process_sync_event(message: dict):
        event_address = w3.toChecksumAddress(
            message["params"]["result"]["address"]
        )
        event_block = int(
            message["params"]["result"]["blockNumber"],
            16,
        )
        event_data = message["params"]["result"]["data"]

        event_reserves = eth_abi.decode(
            ["uint112", "uint112"],
            bytes.fromhex(event_data[2:]),
        )

        v2_pool_helper = None
        for pool_manager in pool_managers.values():
            try:
                v2_pool_helper = pool_manager.get_pool(
                    pool_address=event_address,
                    silent=True,
                )
            except ManagerError:
                continue
            else:
                break

        if v2_pool_helper is None:
            # ignore events for unknown pools
            return

        reserves0, reserves1 = event_reserves

        if TYPE_CHECKING:
            assert isinstance(v2_pool_helper, LiquidityPool)

        try:
            v2_pool_helper.update_reserves(
                external_token0_reserves=reserves0,
                external_token1_reserves=reserves1,
                silent=not VERBOSE_UPDATES,
                print_ratios=False,
                print_reserves=False,
                update_block=event_block,
            )
        except ExternalUpdateError:
            pass
        except:
            logger.exception("(process_sync_event)")

    def process_swap_event(message: dict):
        event_address = w3.toChecksumAddress(
            message["params"]["result"]["address"]
        )
        event_block = int(
            message["params"]["result"]["blockNumber"],
            16,
        )
        event_data = message["params"]["result"]["data"]

        (
            _,
            _,
            event_sqrt_price_x96,
            event_liquidity,
            event_tick,
        ) = eth_abi.decode(
            [
                "int256",
                "int256",
                "uint160",
                "uint128",
                "int24",
            ],
            bytes.fromhex(event_data[2:]),
        )

        v3_pool_helper: Optional[
            Union[V3LiquidityPool, LiquidityPool]
        ] = None
        for pool_manager in pool_managers.values():
            try:
                v3_pool_helper = pool_manager.get_pool(
                    pool_address=event_address,
                    silent=True,
                    state_block=event_block - 1,
                )
            except ManagerError:
                continue
            else:
                break

        if v3_pool_helper is None:
            # ignore events for unknown pools
            return

        if TYPE_CHECKING:
            assert isinstance(v3_pool_helper, V3LiquidityPool)

        try:
            v3_pool_helper.external_update(
                update=UniswapV3PoolExternalUpdate(
                    block_number=event_block,
                    liquidity=event_liquidity,
                    tick=event_tick,
                    sqrt_price_x96=event_sqrt_price_x96,
                ),
            )
        except ExternalUpdateError:
            pass
        except:
            logger.exception("(process_swap_event)")

    def process_new_v2_pool_event(message: dict):
        event_data = message["params"]["result"]["data"]
        event_block = int(message["params"]["result"]["blockNumber"], 16)
        # token0_address = message["params"]["result"]["topics"][1]
        # token1_address = message["params"]["result"]["topics"][2]

        pool_address, _ = eth_abi.decode(
            [
                "address",
                "uint256",
            ],
            bytes.fromhex(event_data[2:]),
        )

        try:
            pool_manager = UniswapV2LiquidityPoolManager(
                factory_address=message["params"]["result"]["address"]
            )
        except:
            return

        try:
            pool_helper = pool_manager.get_pool(
                pool_address=pool_address, state_block=event_block, silent=True
            )
        except Exception as exc:
            print(exc)
            return
        else:
            logger.info(f"Created new V2 pool: {pool_helper}")

    def process_new_v3_pool_event(message: dict):
        event_data = message["params"]["result"]["data"]
        event_block = int(message["params"]["result"]["blockNumber"], 16)
        event_address = message["params"]["result"]["address"]
        # token0_address = message["params"]["result"]["topics"][1]
        # token1_address = message["params"]["result"]["topics"][2]
        # fee = message["params"]["result"]["topics"][3]

        _, pool_address = eth_abi.decode(
            types=("int24", "address"),
            data=bytes.fromhex(event_data[2:]),
        )

        try:
            pool_manager = UniswapV3LiquidityPoolManager(
                factory_address=event_address
            )
        except Exception as exc:
            print(exc)
            return

        try:
            pool_helper = pool_manager.get_pool(
                pool_address=pool_address, state_block=event_block, silent=True
            )
        except Exception as exc:
            print(exc)
            return
        else:
            logger.info(
                f"Created new V3 pool at block {event_block}: {pool_helper} @ {pool_address}"
            )

    _EVENTS = {
        w3.keccak(
            text="Sync(uint112,uint112)",
        ).hex(): {
            "name": "Uniswap V2: SYNC",
            "process_func": process_sync_event,
        },
        w3.keccak(
            text="Mint(address,address,int24,int24,uint128,uint256,uint256)"
        ).hex(): {
            "name": "Uniswap V3: MINT",
            "process_func": process_mint_event,
        },
        w3.keccak(
            text="Burn(address,int24,int24,uint128,uint256,uint256)"
        ).hex(): {
            "name": "Uniswap V3: BURN",
            "process_func": process_burn_event,
        },
        w3.keccak(
            text="Swap(address,address,int256,int256,uint160,uint128,int24)"
        ).hex(): {
            "name": "Uniswap V3: SWAP",
            "process_func": process_swap_event,
        },
        w3.keccak(text="PairCreated(address,address,address,uint256)").hex(): {
            "name": "Uniswap V2: POOL CREATED",
            "process_func": process_new_v2_pool_event,
        },
        w3.keccak(
            text="PoolCreated(address,address,uint24,int24,address)"
        ).hex(): {
            "name": "Uniswap V3: POOL CREATED",
            "process_func": process_new_v3_pool_event,
        },
    }

    async for websocket in websockets.client.connect(
        uri=NODE_WEBSOCKET_URI,
        ping_timeout=None,
        max_queue=None,
    ):
        # reset the status and first block every time we start a new websocket connection
        bot_status.watching_events = False
        bot_status.first_event = 0

        await websocket.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["logs", {}],
                }
            )
        )
        subscription_id = json.loads(await websocket.recv())["result"]
        logger.info(f"Subscription Active: Events - {subscription_id}")
        bot_status.watching_events = True

        while True:
            # process the queue completely before retrieving any more events if bot is "live"
            # does not yield to the event loop, so coroutines will remain suspended until the queue is empty
            if bot_status.live and event_queue:
                event = event_queue.popleft()
                topic0: str = event["params"]["result"]["topics"][0]

                # process the event
                try:
                    process_func = _EVENTS[topic0]["process_func"]
                except KeyError:
                    continue
                else:
                    if TYPE_CHECKING:
                        assert callable(process_func)
                    process_func(event)
                    if VERBOSE_PROCESSING:
                        logger.info(
                            f"processed {_EVENTS[topic0]['name']} event - {len(event_queue)} remaining"
                        )
                    continue

            try:
                message: dict = json.loads(await websocket.recv())
            except asyncio.exceptions.CancelledError:
                return
            except websockets.exceptions.WebSocketException as exc:
                logger.exception(
                    f"(watch_events) (WebSocketException)...\nLatency: {websocket.latency}"
                )
                logger.info("watch_events reconnecting...")
                break
            except:
                logger.exception("(watch_events) (catch-all)")
                sys.exit()

            if not bot_status.first_event:
                bot_status.first_event = int(
                    message["params"]["result"]["blockNumber"],
                    16,
                )
                logger.info(f"First event block: {bot_status.first_event}")

            try:
                message["params"]["result"]["topics"][0]
            except IndexError:
                # ignore anonymous events (no topic0)
                continue
            except:
                logger.exception(f"(event_watcher)\n{message=}")
                continue
            else:
                event_queue.append(message)


async def watch_new_blocks(bot_status: BotStatus):
    """
    Watches the websocket for new blocks, updates the base fee for the last block, scans
    transactions and removes them from the pending tx queue, and prints various messages
    """

    logger.info("Starting block watcher loop")

    # a rolling window of the last 100 block deltas, seeded with an initial value
    block_times = deque(
        [time.time() - bot_status.average_blocktime],
        maxlen=100,
    )

    async for websocket in websockets.client.connect(
        uri=NODE_WEBSOCKET_URI,
        ping_timeout=None,
        max_queue=None,
    ):
        # reset the first block and status every time every time the watcher connects
        bot_status.watching_blocks = False
        bot_status.first_block = 0

        await websocket.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["newHeads"],
                }
            )
        )
        subscription_id = json.loads(await websocket.recv())["result"]
        logger.info(f"Subscription Active: New Blocks - {subscription_id}")
        bot_status.watching_blocks = True

        while True:
            try:
                message = json.loads(await websocket.recv())
            except asyncio.exceptions.CancelledError:
                return
            except websockets.exceptions.WebSocketException:
                logger.exception(
                    "(watch_new_blocks) (websocket.recv) (WebSocketException)"
                )
                break
            except:
                logger.exception(
                    "(watch_new_blocks) (websocket.recv) (catch-all)"
                )
                break

            if VERBOSE_TIMING:
                logger.debug("starting watch_new_blocks")
                start = time.perf_counter()

            bot_status.newest_block = int(
                message["params"]["result"]["number"],
                16,
            )
            bot_status.newest_block_timestamp = int(
                message["params"]["result"]["timestamp"],
                16,
            )

            block_times.append(bot_status.newest_block_timestamp)
            bot_status.average_blocktime = (
                block_times[-1] - block_times[0]
            ) / (len(block_times) - 1)

            if not bot_status.first_block:
                bot_status.first_block = bot_status.newest_block
                logger.info(f"First full block: {bot_status.first_block}")

            (
                bot_status.last_base_fee,
                bot_status.next_base_fee,
            ) = w3.eth.fee_history(1, "latest")["baseFeePerGas"]

            if VERBOSE_BLOCKS:
                logger.info(
                    f"[{bot_status.newest_block}] "
                    + f"base fee: {bot_status.last_base_fee/(10**9):.2f}/{bot_status.next_base_fee/(10**9):.2f} "
                    f"(+{time.time() - bot_status.newest_block_timestamp:.2f}s) "
                )


async def watch_pending_transactions(
    http_session: aiohttp.ClientSession,
    pending_tx: set,
    all_arbs: Dict[str, UniswapLpCycle],
    bot_status: BotStatus,
    process_pool: ProcessPoolExecutor,
):
    while not bot_status.pool_managers_ready:
        await asyncio.sleep(0)

    ROUTERS: Dict[ChecksumAddress, Dict[str, Any]] = {
        # UniswapV2Router02 SushiSwap: Deployer
        w3.toChecksumAddress("0xd9e1cE17f2641f24aE83637ab66a2cca9C378B9F"): {
            "name": "Sushiswap",
        },
        # UniswapV2Router01 Uniswap: Deployer 2
        w3.toChecksumAddress("0xf164fC0Ec4E93095b804a4795bBe1e041497b92a"): {
            "name": "UniswapV2: Router",
        },
        # UniswapV2Router02
        w3.toChecksumAddress("0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"): {
            "name": "UniswapV2: Router 2",
        },
        # SwapRouter
        w3.toChecksumAddress("0xE592427A0AEce92De3Edee1F18E0157C05861564"): {
            "name": "UniswapV3: Router",
        },
        # SwapRouter02
        w3.toChecksumAddress("0x68b3465833fb72A70ecDF485E0e4C7bD8665Fc45"): {
            "name": "UniswapV3: Router 2",
        },
        # UniversalRouter
        w3.toChecksumAddress("0xEf1c6E67703c7BD7107eed8303Fbe6EC2554BF6B"): {
            "name": "Old Universal Router",
        },
        # UniversalRouter
        w3.toChecksumAddress("0x3fC91A3afd70395Cd496C647d5a6CC9D4B2b7FAD"): {
            "name": "New Universal Router",
        },
    }

    for router_address in ROUTERS:
        try:
            router_contract = brownie.Contract(router_address)
        except Exception as e:
            router_contract = brownie.Contract.from_explorer(router_address)

        ROUTERS[router_address]["abi"] = router_contract.abi
        ROUTERS[router_address]["web3_contract"] = w3.eth.contract(
            address=router_address,
            abi=router_contract.abi,
        )

    async for websocket in websockets.client.connect(
        uri=NODE_WEBSOCKET_URI,
        ping_timeout=None,
        max_queue=None,
    ):
        await websocket.send(
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": [
                        "newPendingTransactions",
                        {"includeTransactions": True},
                    ]
                    if SUBSCRIBE_TO_FULL_TRANSACTIONS
                    else ["newPendingTransactions"],
                }
            )
        )
        subscription_id = json.loads(await websocket.recv())["result"]
        logger.info(
            f"Subscription Active: Pending Transactions - {subscription_id}"
        )

        while True:
            if SUBSCRIBE_TO_FULL_TRANSACTIONS:
                transaction = json.loads(await websocket.recv())["params"][
                    "result"
                ]
            else:
                message = await websocket.recv()
                tx_hash = json.loads(message)["params"]["result"]

                try:
                    transaction = w3.eth.get_transaction(tx_hash)
                    # raw_transaction = transaction["raw"]  # for Besu nodes
                    raw_transaction = w3.eth.get_raw_transaction(
                        tx_hash
                    )  # for Geth nodes
                except web3.exceptions.TransactionNotFound as e:
                    # ignore any TX that cannot be found
                    continue

            # Skip if TX already seen, add to set if new
            if tx_hash in pending_tx:
                continue
            else:
                pending_tx.add(tx_hash)

            # Filter contract creation transactions
            if (tx_destination := transaction.get("to")) is None:
                continue

            if tx_destination not in ROUTERS:
                continue

            try:
                params_to_keep = set(
                    [
                        "from",
                        "to",
                        "gas",
                        "maxFeePerGas",
                        "maxPriorityFeePerGas",
                        "gasPrice",
                        "value",
                        "data",
                        "input",
                        "nonce",
                        "type",
                    ]
                )

                if transaction["type"] == "0x0":
                    params_to_keep.remove("maxFeePerGas")
                    params_to_keep.remove("maxPriorityFeePerGas")
                elif transaction["type"] == "0x1":
                    params_to_keep.remove("maxFeePerGas")
                    params_to_keep.remove("maxPriorityFeePerGas")
                elif transaction["type"] == "0x2":
                    params_to_keep.remove("gasPrice")
                else:
                    logger.info(f'Unknown TX type: {transaction["type"]}')
                    logger.info(f"{transaction=}")
                    continue

                w3.eth.call(
                    transaction={
                        k: v
                        for k, v in transaction.items()
                        if k in params_to_keep
                    },
                    block_identifier="latest",
                )
            except web3.exceptions.ContractLogicError as e:
                continue
            except ValueError as e:
                continue
            except Exception as e:
                print(f"{type(e)}:{e}")
                continue

            try:
                tx_input = transaction["input"]
                tx_destination = web3.Web3.toChecksumAddress(tx_destination)
                tx_nonce = transaction["nonce"]
                tx_value = transaction["value"]
                tx_sender = transaction["from"]
                tx_hash = transaction["hash"]
            except KeyError as e:
                continue

            try:
                func_object, func_parameters = w3.eth.contract(
                    address=tx_destination,
                    abi=ROUTERS[tx_destination]["abi"],
                ).decode_function_input(tx_input)
            except ValueError as e:
                # thrown when the function cannot be decoded
                print(e)
                continue
            except Exception as e:
                logger.exception(
                    "(watch_pending_transactions) decode_function_input"
                )
                continue

            try:
                tx_helper = UniswapTransaction(
                    tx_hash=tx_hash,
                    chain_id=brownie.chain.id,
                    func_name=func_object.fn_name,
                    func_params=func_parameters,
                    tx_nonce=tx_nonce,
                    tx_value=tx_value,
                    tx_sender=tx_sender,
                    router_address=tx_destination,
                )
            except TransactionError as e:
                continue

            try:
                sim_results = tx_helper.simulate(silent=True)
            except ManagerError as e:
                logger.info(f"(watch_events.simulate) (ManagerError): {e}")
                continue
            except TransactionError as e:
                logger.debug(
                    f"(watch_events.simulate) (TransactionError): {e}"
                )
                continue

            # Cache the set of pools in the TX
            pool_set = set([pool for pool, _ in sim_results])

            # Find arbitrage helpers that use pools in the TX path
            arb_helpers: List[UniswapLpCycle] = [
                arb
                for arb in all_arbs.values()
                if (
                    # 2pool arbs are always evaluated
                    len(arb.swap_pools) == 2
                    and pool_set.intersection(arb.swap_pools)
                )
                or (
                    len(arb.swap_pools) == 3
                    and pool_set.intersection(
                        # evaluate only 3pool arbs with an affected pool in
                        # the middle position if "REDUCE" mode is active
                        (arb.swap_pools[1],)
                        if REDUCE_TRIANGLE_ARBS
                        else
                        # evaluate all 3pool arbs
                        arb.swap_pools
                    )
                )
            ]

            if not arb_helpers:
                continue

            calculation_futures = []
            for arb_helper in arb_helpers:
                try:
                    calculation_futures.append(
                        await arb_helper.calculate_with_pool(
                            executor=process_pool,
                            override_state=sim_results,
                    )
                )
                except ArbitrageError:
                   pass

            if not calculation_futures:
                return

            calculation_results: List[ArbitrageCalculationResult] = []

            for task in asyncio.as_completed(calculation_futures):
                try:
                    calculation_results.append(await task)
                except ArbitrageError:
                    continue

            # Sort the arb helpers by profit
            all_profitable_calc_results = sorted(
                [
                    calc_result
                    for calc_result in calculation_results
                    if calc_result.profit_amount >= MIN_PROFIT_GROSS
                ],
                key=lambda calc_result: calc_result.profit_amount,
                reverse=True,
            )

            all_profitable_arbs = [
                arb_helper
                for calc_result in all_profitable_calc_results
                if (arb_helper := all_arbs.get(calc_result.id)) is not None
            ]

            # Store results and arbitrage IDs for easy retrieval later
            results_by_arb_id: Dict[str, ArbitrageCalculationResult] = dict()
            for calc_result, arb in zip(
                all_profitable_calc_results,
                all_profitable_arbs,
                strict=True,
            ):
                if TYPE_CHECKING:
                    assert arb is not None
                results_by_arb_id[arb.id] = calc_result

            if not all_profitable_arbs:
                continue

            arbs_without_overlap: Set[UniswapLpCycle] = set()

            while True:
                most_profitable_arb = all_profitable_arbs.pop(0)
                if TYPE_CHECKING:
                    assert most_profitable_arb is not None
                arbs_without_overlap.add(most_profitable_arb)

                conflicting_arbs = [
                    arb_helper
                    for arb_helper in all_profitable_arbs
                    if set(most_profitable_arb.swap_pools)
                    & set(arb_helper.swap_pools)
                ]

                # Drop conflicting arbs from working set
                for arb in conflicting_arbs:
                    all_profitable_arbs.remove(arb)

                # Escape the loop if no arbs remain
                if not all_profitable_arbs:
                    break

            logger.info(
                f"Reduced {len(arb_helpers)} arbs to {len(arbs_without_overlap)}"
            )

            for arb_helper in arbs_without_overlap:
                arb_result = results_by_arb_id[arb_helper.id]

                logger.info(
                    f"Executing arb {arb_helper}, backrunning {tx_hash.hex()}"
                )

                if TYPE_CHECKING:
                    assert isinstance(raw_transaction, HexBytes)

                await execute_arb_with_relay(
                    http_session=http_session,
                    arb_result=arb_result,
                    bot_status=bot_status,
                    all_arbs=all_arbs,
                    state_block=bot_status.newest_block,
                    target_block=bot_status.newest_block + 1,
                    tx_to_backrun=raw_transaction,
                )


if __name__ == "__main__":
    logger = logging.getLogger("ethereum_flashbots_backrun")
    logger.propagate = False
    logger.setLevel(logging.INFO)
    logger_formatter = logging.Formatter("%(levelname)s - %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logger_formatter)
    logger.addHandler(stream_handler)

    os.environ["ETHERSCAN_TOKEN"] = ETHERSCAN_API_KEY

    try:
        brownie.network.connect(BROWNIE_NETWORK)
    except:
        sys.exit(
            "Could not connect! Verify your Brownie network settings using 'brownie networks list'"
        )

    try:
        bot_account = brownie.accounts.load(BROWNIE_ACCOUNT)
        flashbots_id_account = brownie.accounts.load(
            FLASHBOTS_IDENTITY_ACCOUNT
        )
    except:
        sys.exit(
            "Could not load account! Verify your Brownie account settings using 'brownie accounts list'"
        )

    # Create a reusable web3 object to communicate with the node
    # (no arguments to provider will default to localhost on the default port)
    w3 = web3.Web3(web3.HTTPProvider(NODE_HTTP_URI))

    arb_contract = brownie.Contract.from_abi(
        name="",
        address=EXECUTOR_CONTRACT_ADDRESS,
        abi=json.loads(EXECUTOR_CONTRACT_ABI),
    )

    # load historical submitted bundles
    SUBMITTED_BUNDLES = {}
    try:
        with open("submitted_bundles.json") as file:
            SUBMITTED_BUNDLES = json.load(file)
    # if the file doesn't exist, create it
    except FileNotFoundError:
        with open("submitted_bundles.json", "w") as file:
            json.dump(SUBMITTED_BUNDLES, file, indent=2)
    logger.info(f"Found {len(SUBMITTED_BUNDLES)} submitted bundles")

    BLACKLISTED_POOLS = []
    for blacklisted_pools_filename in ["ethereum_blacklisted_pools.json"]:
        try:
            with open(blacklisted_pools_filename, encoding="utf-8") as file:
                BLACKLISTED_POOLS.extend(json.load(file))
        except FileNotFoundError:
            with open(
                blacklisted_pools_filename, "w", encoding="utf-8"
            ) as file:
                json.dump(BLACKLISTED_POOLS, file, indent=2)
    logger.info(f"Found {len(BLACKLISTED_POOLS)} blacklisted pools")

    BLACKLISTED_TOKENS = []
    for blacklisted_tokens_filename in ["ethereum_blacklisted_tokens.json"]:
        try:
            with open(blacklisted_tokens_filename, encoding="utf-8") as file:
                BLACKLISTED_TOKENS.extend(json.load(file))
        except FileNotFoundError:
            with open(
                blacklisted_tokens_filename, "w", encoding="utf-8"
            ) as file:
                json.dump(BLACKLISTED_TOKENS, file, indent=2)
    logger.info(f"Found {len(BLACKLISTED_TOKENS)} blacklisted tokens")

    BLACKLISTED_ARBS = []
    for blacklisted_arbs_filename in ["ethereum_blacklisted_arbs.json"]:
        try:
            with open(blacklisted_arbs_filename, encoding="utf-8") as file:
                BLACKLISTED_ARBS.extend(json.load(file))
        except FileNotFoundError:
            with open(
                blacklisted_arbs_filename, "w", encoding="utf-8"
            ) as file:
                json.dump(BLACKLISTED_ARBS, file, indent=2)
    logger.info(f"Found {len(BLACKLISTED_ARBS)} blacklisted arbs")

    start = time.perf_counter()
    asyncio.run(
        main(),
        # debug=True,
    )
    logger.info(f"Completed in {time.perf_counter() - start:.2f}s")