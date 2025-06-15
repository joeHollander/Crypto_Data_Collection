# using cryptofeed ingest data into ArcticDB
# first using OKX or Kraken with BTCUSDT or BTCUSD
# then more
# then connect to VPS

import asyncio
import pandas as pd
from pathlib import Path
from cryptofeed import FeedHandler
from cryptofeed.callback import OrderInfoCallback
from cryptofeed.exchanges import Kraken, OKX, Coinbase, BinanceUS, Gemini, Bitstamp, dYdX
from cryptofeed.defines import TRADES, OKX, ORDER_INFO
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
    
    lib.append(trade.symbol, df)
    
    # confirmation of data 
    print(f"Stored: {trade.exchange} | {trade.symbol} | {trade.side} | {trade.amount:.6f} @ {trade.price:.2f} = {(trade.amount * trade.price):.2f}" )

async def order(oi, receipt_timestamp):
    print(f"Order update received at {receipt_timestamp}: {oi}")

def main(symbols):
    """
    Main function to set up and run the cryptofeed feed.
    """
    f = FeedHandler(config="config.yaml")
    
    # custom call back function 
    f.add_feed(Kraken(symbols=symbols, channels=[TRADES], callbacks={TRADES: custom_arctic_callback}))
    f.add_feed(BinanceUS(symbols=symbols, channels=[TRADES], callbacks={TRADES: custom_arctic_callback}))
    f.add_feed(Gemini(symbols=symbols, channels=[TRADES], callbacks={TRADES: custom_arctic_callback}))
    
    #f.add_feed(OKX, channels=[ORDER_INFO], symbols=["ETH-USDT-PERP", "BTC-USDT-PERP"], callbacks={ORDER_INFO: OrderInfoCallback(order)}, timeout=-1)

    print("\nStarting data ingestion. Press Ctrl+C to stop.")
    f.run()


if __name__ == '__main__':
    try:
        main(["BTC-USD"])
    except KeyboardInterrupt:
        print("\nData ingestion stopped by user.")