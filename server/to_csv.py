import os
import pandas as pd
from arcticdb import Arctic
import io

# --- Configuration ---
# The path to the directory containing your data.mdb file.
# Assumes the script is in a parent folder to 'crypto_data'.
DB_PATH = "lmdb://crypto_data"
# The name of the library you created in your collector script.
LIBRARY_NAME = "rest_data"
info = ("cryptocom", "BTC_USD", 1)
sym = f"{info[0]}_{info[1]}_{info[2]}s"

ac = Arctic(DB_PATH)
lib = ac[LIBRARY_NAME]

def get_data_as_csv_bytes(symbol: str) -> bytes:
    """Helper function to get a symbol's data as CSV bytes."""
    try:
        df = lib.read(symbol).data # .data unwraps the VersionedItem
        # Convert to CSV in memory
        string_buffer = io.StringIO()
        df.to_csv(string_buffer, index=True)
        # Return as UTF-8 encoded bytes
        return string_buffer.getvalue().encode('utf-8')
    except Exception:
        # Return an error message as bytes if symbol not found
        return f"Error: Data for symbol '{symbol}' not found.".encode('utf-8')