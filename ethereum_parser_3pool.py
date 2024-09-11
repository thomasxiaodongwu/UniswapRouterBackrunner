import json
import web3
import networkx as nx
import itertools
import time

WETH_ADDRESS = "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"

start_timer = time.perf_counter()

BLACKLISTED_TOKENS = [
    # "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",  # USDC
    # "0xdAC17F958D2ee523a2206206994597C13D831ec7",  # USDT
]

v2_lp_data = {}
for filename in [
    "ethereum_lps_sushiswapv2.json",
    "ethereum_lps_uniswapv2.json",
]:
    with open(filename) as file:
        for pool in json.load(file):
            v2_lp_data[pool.get("pool_address")] = {
                key: value
                for key, value in pool.items()
                if key not in ["pool_id"]
            }
print(f"Found {len(v2_lp_data)} V2 pools")

v3_lp_data = {}
for filename in [
    "ethereum_lps_sushiswapv3.json",
    "ethereum_lps_uniswapv3.json",
]:
    with open(filename) as file:
        for pool in json.load(file):
            v3_lp_data[pool.get("pool_address")] = {
                key: value
                for key, value in pool.items()
                if key not in ["block_number"]
            }
print(f"Found {len(v3_lp_data)} V3 pools")

all_v2_pools = set(v2_lp_data.keys())
all_v3_pools = set(v3_lp_data.keys())

all_tokens = set(
    [lp.get("token0") for lp in v2_lp_data.values()]
    + [lp.get("token1") for lp in v2_lp_data.values()]
    + [lp.get("token0") for lp in v3_lp_data.values()]
    + [lp.get("token1") for lp in v3_lp_data.values()]
)


# build the graph with tokens as nodes, adding an edge
# between any two tokens held by a liquidity pool
G = nx.MultiGraph()
for pool in v2_lp_data.values():
    G.add_edge(
        pool.get("token0"),
        pool.get("token1"),
        lp_address=pool.get("pool_address"),
        pool_type="UniswapV2",
    )

for pool in v3_lp_data.values():
    G.add_edge(
        pool.get("token0"),
        pool.get("token1"),
        lp_address=pool.get("pool_address"),
        pool_type="UniswapV3",
    )

# delete nodes for blacklisted tokens
G.remove_nodes_from(BLACKLISTED_TOKENS)

print(f"G ready: {len(G.nodes)} nodes, {len(G.edges)} edges")

all_tokens_with_weth_pool = list(G.neighbors(WETH_ADDRESS))
print(f"Found {len(all_tokens_with_weth_pool)} tokens with a WETH pair")

print("*** Finding triangular arbitrage paths ***")
triangle_arb_paths = {}

# only consider tokens with degree > 1 (number of pools holding the token)
filtered_tokens = [
    token for token in all_tokens_with_weth_pool if G.degree(token) > 1
]
print(f"Processing {len(filtered_tokens)} tokens with degree > 1")

# loop through all possible token pair
for token_a, token_b in itertools.combinations(filtered_tokens, 2):
    # find tokenA/tokenB pools, skip if a tokenA/tokenB pool is not found
    if not G.get_edge_data(token_a, token_b):
        continue

    inside_pools = [
        edge.get("lp_address")
        for edge in G.get_edge_data(token_a, token_b).values()
    ]

    # find tokenA/WETH pools
    outside_pools_tokenA = [
        edge.get("lp_address")
        for edge in G.get_edge_data(token_a, WETH_ADDRESS).values()
    ]

    # find tokenB/WETH pools
    outside_pools_tokenB = [
        edge.get("lp_address")
        for edge in G.get_edge_data(token_b, WETH_ADDRESS).values()
    ]

    # find all triangular arbitrage paths of form:
    # tokenA/WETH -> tokenA/tokenB -> tokenB/WETH
    for pool_addresses in itertools.product(
        outside_pools_tokenA, inside_pools, outside_pools_tokenB
    ):
        pool_data = {}
        for pool_address in pool_addresses:
            if pool_address in all_v2_pools:
                pool_info = {
                    pool_address: {
                        key: value
                        for key, value in v2_lp_data.get(pool_address).items()
                    }
                }
            elif pool_address in all_v3_pools:
                pool_info = {
                    pool_address: {
                        key: value
                        for key, value in v3_lp_data.get(pool_address).items()
                    }
                }
            else:
                raise Exception
            pool_data.update(pool_info)

        triangle_arb_paths[id] = {
            "id": (
                id := web3.Web3.keccak(
                    hexstr="".join(
                        [pool_address[2:] for pool_address in pool_addresses]
                    )
                ).hex()
            ),
            "path": pool_addresses,
            "pools": pool_data,
        }

    # find all triangular arbitrage paths of form:
    # tokenB/WETH -> tokenA/tokenB -> tokenA/WETH
    for pool_addresses in itertools.product(
        outside_pools_tokenB, inside_pools, outside_pools_tokenA
    ):
        pool_data = {}
        for pool_address in pool_addresses:
            if pool_address in all_v2_pools:
                pool_info = {
                    pool_address: {
                        key: value
                        for key, value in v2_lp_data.get(pool_address).items()
                    }
                }
            elif pool_address in all_v3_pools:
                pool_info = {
                    pool_address: {
                        key: value
                        for key, value in v3_lp_data.get(pool_address).items()
                    }
                }
            else:
                raise Exception
            pool_data.update(pool_info)

        triangle_arb_paths[id] = {
            "id": (
                id := web3.Web3.keccak(
                    hexstr="".join(
                        [pool_address[2:] for pool_address in pool_addresses]
                    )
                ).hex()
            ),
            "path": pool_addresses,
            "pools": pool_data,
        }

print(
    f"Found {len(triangle_arb_paths)} triangle arb paths in {time.perf_counter() - start_timer:.1f}s"
)

print("• Saving arb paths to JSON")
with open("ethereum_arbs_3pool.json", "w") as file:
    json.dump(triangle_arb_paths, file, indent=2)