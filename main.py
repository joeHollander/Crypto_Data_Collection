import asyncio
import ccxt.async_support as ccxt
from arcticdb import Arctic
import pandas as pd
import numpy as np

# NOTES
# could compile best bids from multiple exchange
# VWAP determining dollar amount could also be an amount of the currency
# could use different exchange


# connecting to arctic database
lmdb_uri = "lmdb://crypto_data"
ac = Arctic(lmdb_uri)

# creates library if it doesn't exist
# delete "_test" in prod
lib = ac.get_library("rest_data_test", create_if_missing=True)

def vwap(orders, cost=100_000):
    # unpack list of lists array
    prices = orders[:, 0]
    quantities = orders[:, 1]

    # cumulative cost and quantity at each level
    orders_cost = prices * quantities
    cum_cost = np.cumsum(orders_cost)
    cum_quantity = np.cumsum(quantities)

    # check if book values is less than cost
    if cum_cost[-1] < cost:
        actual_cost = cum_cost[-1]
        vwap_price = actual_cost / cum_quantity[-1]
        return (vwap_price, actual_cost)

    actual_cost = cost
    
    # determine level needed to supply cost
    idx = np.argmax(cum_cost >= cost)

    # calculate vwap
    if idx == 0:
        return (prices[0], actual_cost)
    else:
        # cost for all full levels
        cost_of_full_levels = cum_cost[idx - 1]
        
        # quantity acquired from all full levels
        quantity_from_full_levels = cum_quantity[idx - 1]

        # money left to spend on the final partial level
        remaining_cost = cost - cost_of_full_levels

        # price of the final partial level
        price_of_partial_level = prices[idx]

        # quantity we can buy with the remaining money at that price
        quantity_from_partial_level = remaining_cost / price_of_partial_level
        
        total_quantity_acquired = quantity_from_full_levels + quantity_from_partial_level
        
        vwap_price = cost / total_quantity_acquired

    return (vwap_price, actual_cost)

# fetches order book data 
async def data(exchange_id = "cryptocom", symbol = "BTC/USD", interval=1):
    # initializing exchange
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class()
    await exchange.load_markets()

    print(f"starting {interval} second data collection for {symbol}")
    print("-" * 60)

    prev_timestamp = None

    try:
        while True:
            try:
                order_book = await exchange.fetchL2OrderBook(symbol)
                if order_book["bids"] and order_book["asks"]:
                    bids = np.array(order_book["bids"])
                    asks = np.array(order_book["asks"])
                    timestamp = order_book["timestamp"]

                    if prev_timestamp is not None:
                        real_interval = order_book["timestamp"] - prev_timestamp
                    else:
                        real_interval = 0

                    prev_timestamp = timestamp
                    
                    print(vwap(bids)[0])

                    data = {
                        'timestamp': [timestamp],
                        'exchange': [exchange_id],
                        'symbol': [symbol],
                        'best_bid': [float(bids[0][0])],
                        'bid_vwap': [float(round(vwap(bids)[0], 2))],
                        'bid_vwap_cost': [float(round(vwap(bids)[1], 2))],
                        'best_ask': [float(asks[0][0])],
                        'ask_vwap': [float(round(vwap(asks)[0], 2))],
                        'ask_vwap_cost': [float(round(vwap(asks)[1], 2))],
                        }
                    
                    df = pd.DataFrame(data)
                  
                    lib.append(symbol, df)
                    
                    print(f"Timestamp: {timestamp} | Best Bid: {bids[0][0]} | Best Ask: {asks[0][0]} | Real Interval: {real_interval / 1e3}") 
                    
                else:
                    print("Could not retrieve bid/ask. The order book might be empty.")
            except ccxt.NetworkError as e:
                print(f"A network error occurred: {e}. Retrying in 5 seconds...")
                await asyncio.sleep(5)
            except ccxt.ExchangeError as e:
                print(f"An exchange error occurred: {e}. Stopping.")
                break
            except Exception as e:
                print(f"An unexpected error occurred: {e}")
                break
            
            # rest for interval
            await asyncio.sleep(interval)
    finally:
        print("-" * 60)
        print("closing connection")
        await exchange.close()

if __name__ == '__main__':
    try:
        asyncio.run(data())
    except KeyboardInterrupt:
        print("\nData ingestion stopped by user.")