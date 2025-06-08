from arcticdb import Arctic
import pandas as pd
import numpy as np
from pathlib import Path

db_path = Path.home() / "cryptofeed_data_manual" / "arctic_db"
ARCTIC_LMDB_URI = f"lmdb://{db_path.as_posix()}"
store = Arctic(ARCTIC_LMDB_URI)
lib = store["cryptofeed_trades"]
data = lib.read("BTC-USD").data

orig_time = data.iloc[0, -1]
data["s_since"] = data["receipt_timestamp"] - orig_time
print(data)
