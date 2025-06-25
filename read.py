from arcticdb import Arctic
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt

ARCTIC_LMDB_URI = f"lmdb://crypto_data"
store = Arctic(ARCTIC_LMDB_URI)
lib = store["rest_data_test_concurrent"]
print(lib.list_symbols())
eth = lib["cryptocom_ETH_USD"].data
btc = lib["cryptocom_BTC_USD"].data

fig, ax = plt.subplots(1, 2)

ax[0].plot(btc["timestamp"], btc["best_ask"])
ax[0].set_title("BTC/USD")

ax[1].plot(eth["timestamp"], eth["best_ask"])
ax[1].set_title("ETH/USD")

plt.show()