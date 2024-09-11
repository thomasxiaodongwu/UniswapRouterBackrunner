import brownie
import sys
import os
import json

BROWNIE_NETWORK = "mainnet-local"
os.environ["ETHERSCAN_TOKEN"] = "EDITME"

# maximum blocks to process with getLogs
BLOCK_SPAN = 1_000

try:
    brownie.network.connect(BROWNIE_NETWORK)
except:
    sys.exit("Could not connect!")

exchanges = [
    {
        "name": "SushiSwap",
        "filename": "ethereum_lps_sushiswapv2.json",
        "factory_address": "0xC0AEe478e3658e2610c5F7A4A2E1777cE9e4f2Ac",
        "factory_deployment_block": 10_794_229,
        "pool_type": "SushiswapV2",
    },
    {
        "name": "Uniswap V2",
        "filename": "ethereum_lps_uniswapv2.json",
        "factory_address": "0x5C69bEe701ef814a2B6a3EDD4B1652CB9cc5aA6f",
        "factory_deployment_block": 10_000_835,
        "pool_type": "UniswapV2",
    },
]

current_block = brownie.chain.height

for name, factory_address, filename, deployment_block, pool_type in [
    (
        exchange["name"],
        exchange["factory_address"],
        exchange["filename"],
        exchange["factory_deployment_block"],
        exchange["pool_type"],
    )
    for exchange in exchanges
]:
    print(f"DEX: {name}")

    try:
        factory_contract = brownie.Contract(factory_address)
    except:
        try:
            factory_contract = brownie.Contract.from_explorer(factory_address)
        except:
            factory_contract = None
    finally:
        if factory_contract is None:
            sys.exit("FACTORY COULD NOT BE LOADED")

    try:
        with open(filename) as file:
            lp_data = json.load(file)
    except FileNotFoundError:
        lp_data = []

    if lp_data:
        previous_pool_count = len(lp_data)
        print(f"Found previously-fetched data: {previous_pool_count} pools")
        previous_block = lp_data[-1].get("block_number")
        print(f"Found pool data up to block {previous_block}")
    else:
        previous_pool_count = 0
        previous_block = deployment_block

    for i in range(previous_block + 1, current_block + 1, BLOCK_SPAN):
        if i + BLOCK_SPAN > current_block:
            end_block = current_block
        else:
            end_block = i + BLOCK_SPAN

        if pool_created_events := factory_contract.events.PairCreated.getLogs(
            fromBlock=i, toBlock=end_block
        ):
            for event in pool_created_events:
                lp_data.append(
                    {
                        "pool_address": event.args.get("pair"),
                        "token0": event.args.get("token0"),
                        "token1": event.args.get("token1"),
                        "block_number": event.get("blockNumber"),
                        "pool_id": event.args.get(""),
                        "type": pool_type,
                    }
                )
        with open(filename, "w") as file:
            json.dump(lp_data, file, indent=2)

    print(
        f"Saved {len(lp_data)} pools ({len(lp_data) - previous_pool_count} new)"
    )