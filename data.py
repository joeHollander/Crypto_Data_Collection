# using cryptofeed ingest data into ArcticDB
# first using OKX or Kraken with BTCUSDT or BTCUSD
# then more
# then connect to VPS

import asyncio
import pandas as pd
from pathlib import Path
from cryptofeed import FeedHandler
from cryptofeed.exchanges import Kraken, OKX
from cryptofeed.defines import TRADES
from arcticdb import Arctic  # We now import ArcticDB directly



path = "cryptofeed_data"
ARCTIC_LMDB_URI = "lmdb://" + path
print(f"Using ArcticDB LMDB URI: {ARCTIC_LMDB_URI}")

# 4. Connect to ArcticDB and get the library handle
# We do this once at the start for efficiency.
ac = Arctic(ARCTIC_LMDB_URI)
lib = ac.get_library('cryptofeed_trades', create_if_missing=True)
print(f"Connected to library 'cryptofeed_trades'.")


# --- THIS IS OUR NEW CUSTOM CALLBACK ---
async def custom_arctic_callback(trade, receipt_timestamp):
    """
    This function is called by cryptofeed for every trade.
    It manually converts the data and writes it to ArcticDB.
    """
    # The 'trade' object is a dictionary-like object from cryptofeed
    # We create a dictionary with the data we want to store
    data = {
    'exchange': [trade.exchange],
    'symbol': [trade.symbol],
    'side': [trade.side],
    'amount': [float(trade.amount)],  # Convert Decimal to float
    'price': [float(trade.price)],    # Convert Decimal to float
    'receipt_timestamp': [receipt_timestamp]
    }
    # The timestamp from the exchange is the natural index for time-series data
    df = pd.DataFrame(data, index=[pd.to_datetime(trade.timestamp, unit='s')])
    
    # We use the library handle we created earlier to write the data
    # The symbol name is a unique identifier for the data stream
    # Using 'append' is efficient for writing streaming data.
    lib.append(trade.symbol, df)
    
    # Optional: Print a confirmation to see it working in real-time
    print(f"Stored: {trade.symbol} | {trade.side} | {trade.amount:.6f} @ {trade.price:.2f} = {(trade.amount * trade.price):.2f}" )


def main():
    """
    Main function to set up and run the cryptofeed feed.
    """
    f = FeedHandler()
    
    # Here, we pass our OWN function to the callbacks dictionary
    f.add_feed(Kraken(
        symbols=['BTC-USD'],
        channels=[TRADES],
        callbacks={TRADES: custom_arctic_callback}
    ))

    f.add_feed(OKX(
        symbols=['BTC-USD'],
        channels=[TRADES],
        callbacks={TRADES: custom_arctic_callback}
    ))


    print("\nStarting data ingestion with custom callback. Press Ctrl+C to stop.")
    f.run()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\nData ingestion stopped by user.")