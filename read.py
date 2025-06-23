from arcticdb import Arctic
import pandas as pd
import numpy as np
from pathlib import Path

ARCTIC_LMDB_URI = f"lmdb://crypto_data"
store = Arctic(ARCTIC_LMDB_URI)
lib = store["rest_data_test"]
print(lib["BTC/USD"].data)
