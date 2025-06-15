from arcticdb import Arctic
import pandas as pd
import numpy as np
from pathlib import Path

ARCTIC_LMDB_URI = f"lmdb://cryptofeed_data"
store = Arctic(ARCTIC_LMDB_URI)
lib = store["cryptofeed_trades"]
print(lib.list_symbols())
data = lib.read("BTC-USD").data

print(data)