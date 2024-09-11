import json
import web3
import networkx as nx
import itertools

WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

BLACKLISTED_TOKENS = [
    # add addresses here if you want to exclude a token from consideration during pathfinding
    # e.g.
    # "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
    # "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
]

sushiv2_lp_data = {}
univ2_lp_data = {}
sushiv3_lp_data = {}
univ3_lp_data = {}

with open("ethereum_lps_sushiswapv2.json") as file:
    for pool in json.load(file):
        sushiv2_lp_data[pool.get("pool_address")] = {
            key: value for key, value in pool.items() if key not in ["pool_id"]
        }
print(f"Found {len(sushiv2_lp_data)} Sushiswap V2 pools")

with open("ethereum_lps_uniswapv2.json") as file:
    for pool in json.load(file):
        univ2_lp_data[pool.get("pool_address")] = {
            key: value for key, value in pool.items() if key not in ["pool_id"]
        }
print(f"Found {len(univ2_lp_data)} Uniswap V2 pools")


with open("ethereum_lps_sushiswapv3.json") as file:
    for pool in json.load(file):
        sushiv3_lp_data[pool.get("pool_address")] = {
            key: value
            for key, value in pool.items()
            if key not in ["block_number"]
        }
print(f"Found {len(sushiv3_lp_data)} Sushiswap V3 pools")


with open("ethereum_lps_uniswapv3.json") as file:
    for pool in json.load(file):
        univ3_lp_data[pool.get("pool_address")] = {
            key: value
            for key, value in pool.items()
            if key not in ["block_number"]
        }
print(f"Found {len(univ3_lp_data)} Uniswap V3 pools")


# build the graph with tokens as nodes, adding an edge
# between any two tokens held by a liquidity pool
G = nx.MultiGraph()
for pool in univ2_lp_data.values():
    G.add_edge(
        pool.get("token0"),
        pool.get("token1"),
        lp_address=pool.get("pool_address"),
        pool_type="UniswapV2",
    )

for pool in sushiv2_lp_data.values():
    G.add_edge(
        pool.get("token0"),
        pool.get("token1"),
        lp_address=pool.get("pool_address"),
        pool_type="SushiswapV2",
    )

for pool in univ3_lp_data.values():
    G.add_edge(
        pool.get("token0"),
        pool.get("token1"),
        lp_address=pool.get("pool_address"),
        pool_type="UniswapV3",
    )


for pool in sushiv3_lp_data.values():
    G.add_edge(
        pool.get("token0"),
        pool.get("token1"),
        lp_address=pool.get("pool_address"),
        pool_type="SushiswapV3",
    )

# delete nodes for blacklisted tokens
G.remove_nodes_from(BLACKLISTED_TOKENS)

print(f"G ready: {len(G.nodes)} nodes, {len(G.edges)} edges")

all_tokens_with_weth_pool = list(G.neighbors(WETH_ADDRESS))
print(f"Found {len(all_tokens_with_weth_pool)} tokens with a WETH pair")

print("*** Finding two-pool arbitrage paths ***")
two_pool_arb_paths = {}

for token in all_tokens_with_weth_pool:
    pools = G.get_edge_data(token, WETH_ADDRESS).values()

    # skip tokens with only one pool
    if len(pools) < 2:
        continue

    for pool_a, pool_b in itertools.permutations(pools, 2):
        pool_a_address = pool_a["lp_address"]
        pool_b_address = pool_b["lp_address"]
        pool_a_type = pool_a["pool_type"]
        pool_b_type = pool_b["pool_type"]

        if pool_a_type == "UniswapV2":
            pool_a_dict = univ2_lp_data[pool_a_address]
        elif pool_a_type == "SushiswapV2":
            pool_a_dict = sushiv2_lp_data[pool_a_address]
        elif pool_a_type == "UniswapV3":
            pool_a_dict = univ3_lp_data[pool_a_address]
        elif pool_a_type == "SushiswapV3":
            pool_a_dict = sushiv3_lp_data[pool_a_address]
        else:
            raise Exception(f"could not identify pool {pool_a}")

        if pool_b_type == "UniswapV2":
            pool_b_dict = univ2_lp_data[pool_b_address]
        elif pool_b_type == "SushiswapV2":
            pool_b_dict = sushiv2_lp_data[pool_b_address]
        elif pool_b_type == "UniswapV3":
            pool_b_dict = univ3_lp_data[pool_b_address]
        elif pool_b_type == "SushiswapV3":
            pool_b_dict = sushiv3_lp_data[pool_b_address]
        else:
            raise Exception(f"could not identify pool {pool_b}")

        two_pool_arb_paths[id] = {
            "id": (
                id := web3.Web3.keccak(
                    hexstr="".join(
                        [
                            pool_a.get("lp_address")[2:],
                            pool_b.get("lp_address")[2:],
                        ]
                    )
                ).hex()
            ),
            "pools": {
                pool_a.get("lp_address"): pool_a_dict,
                pool_b.get("lp_address"): pool_b_dict,
            },
            "arb_types": ["cycle", "flash_borrow_lp_swap"],
            "path": [pool.get("lp_address") for pool in [pool_a, pool_b]],
        }
print(f"Found {len(two_pool_arb_paths)} unique two-pool arbitrage paths")

print("• Saving arb paths to JSON")
with open("ethereum_arbs_2pool.json", "w") as file:
    json.dump(two_pool_arb_paths, file, indent=2)