import brownie
import sys
import os
import json

BROWNIE_NETWORK = "mainnet-local"
os.environ["ETHERSCAN_TOKEN"] = EDITME

# maximum blocks to process with getLogs
BLOCK_SPAN = 5_000

try:
    brownie.network.connect(BROWNIE_NETWORK)
except:
    sys.exit("Could not connect!")

exchanges = [
    {
        "name": "Uniswap V3",
        "filename": "ethereum_lps_uniswapv3.json",
        "factory_address": "0x1F98431c8aD98523631AE4a59f267346ea31F984",
        "factory_deployment_block": 12_369_621,
        "pool_type": "UniswapV3",
    },
    {
        "name": "Sushiswap V3",
        "filename": "ethereum_lps_sushiswapv3.json",
        "factory_address": "0xbACEB8eC6b9355Dfc0269C18bac9d6E2Bdc29C4F",
        "factory_deployment_block": 16_955_547,
        "pool_type": "SushiswapV3",
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
        factory = brownie.Contract(factory_address)
    except:
        try:
            factory = brownie.Contract.from_explorer(factory_address)
        except:
            factory = None
    finally:
        if factory is None:
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

    factory_contract = brownie.web3.eth.contract(
        address=factory.address, abi=factory.abi
    )

    for i in range(previous_block + 1, current_block + 1, BLOCK_SPAN):
        if i + BLOCK_SPAN > current_block:
            end_block = current_block
        else:
            end_block = i + BLOCK_SPAN

        if pool_created_events := factory_contract.events.PoolCreated.getLogs(
            fromBlock=i, toBlock=end_block
        ):
            for event in pool_created_events:
                lp_data.append(
                    {
                        "pool_address": event.args.pool,
                        "fee": event.args.fee,
                        "token0": event.args.token0,
                        "token1": event.args.token1,
                        "block_number": event.blockNumber,
                        "type": pool_type,
                    }
                )
        with open(filename, "w") as file:
            json.dump(lp_data, file, indent=2)

    print(
        f"Saved {len(lp_data)} pools ({len(lp_data) - previous_pool_count} new)"
    )