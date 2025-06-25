import asyncio
import ccxt.async_support as ccxt
from arcticdb import Arctic
import pandas as pd
import numpy as np

# --- ArcticDB Setup ---
# connecting to arctic database
# Using a local LMDB database for storing the data.
lmdb_uri = "lmdb://crypto_data"
ac = Arctic(lmdb_uri)

# creates library if it doesn't exist
# Using a separate library for this test run.
lib = ac.get_library("rest_data_test_concurrent", create_if_missing=True)

def vwap(orders, cost=100_000):
    """
    Calculates the Volume-Weighted Average Price (VWAP) for a given cost.
    
    Args:
        orders (np.array): A numpy array of [price, quantity] pairs.
        cost (float): The total dollar amount to calculate the VWAP for.
        
    Returns:
        tuple: A tuple containing (vwap_price, actual_cost_spent).
    """
    # Unpack the list of lists into separate price and quantity arrays.
    prices = orders[:, 0]
    quantities = orders[:, 1]

    # Calculate the cumulative cost and quantity at each level of the order book.
    orders_cost = prices * quantities
    cum_cost = np.cumsum(orders_cost)
    cum_quantity = np.cumsum(quantities)

    # If the total value of the order book is less than the desired cost,
    # calculate the VWAP for the entire book.
    if cum_cost[-1] < cost:
        actual_cost = cum_cost[-1]
        vwap_price = actual_cost / cum_quantity[-1]
        return (vwap_price, actual_cost)

    actual_cost = cost
    
    # Find the first level in the cumulative cost array that meets or exceeds the target cost.
    idx = np.argmax(cum_cost >= cost)

    # If the first level is enough, the VWAP is just the price of that level.
    if idx == 0:
        return (prices[0], actual_cost)
    else:
        # Calculate the cost and quantity from all the full levels we will consume.
        cost_of_full_levels = cum_cost[idx - 1]
        quantity_from_full_levels = cum_quantity[idx - 1]

        # Calculate how much money is left to spend on the final, partial level.
        remaining_cost = cost - cost_of_full_levels
        price_of_partial_level = prices[idx]
        quantity_from_partial_level = remaining_cost / price_of_partial_level
        
        # The total quantity is the sum from full levels and the partial level.
        total_quantity_acquired = quantity_from_full_levels + quantity_from_partial_level
        
        # VWAP is the total cost divided by the total quantity acquired.
        vwap_price = cost / total_quantity_acquired

    return (vwap_price, actual_cost)

async def data_collector_task(exchange, exchange_id, symbol, interval):
    """
    The core data collection loop. It receives an already initialized exchange instance
    and is responsible only for fetching and storing data.
    """
    log_prefix = f"[{exchange_id.upper()} - {symbol}]"
    print(f"{log_prefix} Starting {interval}s data collection.")

    try:
        while True:
            try:
                # Fetch the Level 2 order book.
                order_book = await exchange.fetchL2OrderBook(symbol)
                
                # Ensure the order book has both bids and asks.
                if order_book["bids"] and order_book["asks"]:
                    bids = np.array(order_book["bids"])
                    asks = np.array(order_book["asks"])
                    timestamp = order_book["timestamp"]

                    bid_vwap_price, bid_vwap_cost = vwap(bids)
                    ask_vwap_price, ask_vwap_cost = vwap(asks)

                    data_dict = {
                        'timestamp': [timestamp],
                        'exchange': [exchange_id],
                        'symbol': [symbol],
                        'best_bid': [float(bids[0][0])],
                        'bid_vwap': [float(round(bid_vwap_price, 2))],
                        'bid_vwap_cost': [float(round(bid_vwap_cost, 2))],
                        'best_ask': [float(asks[0][0])],
                        'ask_vwap': [float(round(ask_vwap_price, 2))],
                        'ask_vwap_cost': [float(round(ask_vwap_cost, 2))],
                    }
                    df = pd.DataFrame(data_dict)
                    db_symbol = f"{exchange_id}_{symbol.replace('/', '_')}"
                    lib.append(db_symbol, df)
                    
                    print(f"{log_prefix} Time: {timestamp} | Bid: {bids[0][0]} | Ask: {asks[0][0]}") 
                    
                else:
                    print(f"{log_prefix} Could not retrieve bid/ask. The order book might be empty.")

            except ccxt.NetworkError as e:
                print(f"{log_prefix} A network error occurred: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except ccxt.ExchangeError as e:
                print(f"{log_prefix} An exchange error occurred: {e}. Stopping task.")
                break 
            except Exception as e:
                print(f"{log_prefix} An unexpected error occurred: {e}. Stopping task.")
                break
            
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        print(f"{log_prefix} Collection task cancelled.")
    finally:
        print(f"{log_prefix} Data collection loop finished.")


async def main(tasks_to_run):
    """
    Initializes all exchange connections, runs the data collection tasks,
    and ensures all connections are closed on exit. This function acts
    as a resource manager.
    """
    exchanges = {}
    try:
        # Initialize exchange instances and create coroutines to load their markets.
        print("Initializing exchange connections...")
        init_coros = []
        for eid in {task[0] for task in tasks_to_run}:
             exchange_class = getattr(ccxt, eid)
             exchanges[eid] = exchange_class()
             init_coros.append(exchanges[eid].load_markets())
        
        # Load all market data concurrently.
        await asyncio.gather(*init_coros)
        print("Exchange connections initialized successfully.")
        print("-" * 60)

        # Create and run the data collector tasks.
        collector_tasks = [
            data_collector_task(exchanges[eid], eid, sym, interval)
            for eid, sym, interval in tasks_to_run
        ]
        await asyncio.gather(*collector_tasks)

    finally:
        # This block will run on normal exit, on exception, or on cancellation (Ctrl+C).
        print("-" * 60)
        print("Shutting down. Closing all exchange connections...")
        close_coros = [ex.close() for ex in exchanges.values()]
        if close_coros:
            await asyncio.gather(*close_coros, return_exceptions=True)
        print("All connections closed.")

if __name__ == '__main__':
    # --- Configuration ---
    # Define the list of data collection tasks you want to run.
    # Each tuple is (exchange_id, symbol, interval_in_seconds)
    tasks = [
        ('cryptocom', 'ETH/USD', 1),
        ('cryptocom', 'BTC/USD', 1)
    ]

    try:
        print("Starting data ingestion for multiple symbols... (Press Ctrl+C to stop)")
        asyncio.run(main(tasks))
    except KeyboardInterrupt:
        # This message is shown after asyncio.run() has completed its cleanup.
        print("\nData ingestion process stopped by user.")
