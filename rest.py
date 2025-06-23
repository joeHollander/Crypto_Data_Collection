import asyncio
import ccxt.async_support as ccxt
import os
import numpy as np

x=0

# determines cost (price * quantity) at each order book level
def cost(price, quantity):
    return price * quantity

def vwap(orders, cost=100_000):
    # transforming order format
    unpacked_orders = np.array(list(zip(*orders)))
    # finding cost
    orders_cost = unpacked_orders[0] * unpacked_orders[1]
    # cumulative cost
    cum_cost = np.cumsum(orders_cost)
    print(cum_cost)
    # index of exceeding cost level
    idx = np.argmax(cum_cost >= cost)
    # cumulative quantity
    cum_quantity = np.cumsum(unpacked_orders[1])
    # extra cost if last order level is bought
    extra_cost = (cum_cost[idx] - cost)
    # calculation
    return cost/(cum_quantity[idx-1] + extra_cost/unpacked_orders[0][idx])

    orders


async def poll_coinbase_bid_ask():
    """
    Fetches and prints the best bid and ask from Coinbase every second.
    """
    # Instantiate the exchange with your API keys for authenticated requests
    exchange = ccxt.cryptocom()

    print("Starting one-second polling for BTC/USD bid/ask from Crypto.COM ...")
    print("-" * 60)

    global x

    try:
        while True:
            try:
                # fetch top of order_book
                order_book = await exchange.fetchL2OrderBook('BTC/USD')

                if order_book['bids'] and order_book['asks']:
                    bids = np.array(order_book["bids"])
                    asks = np.array(order_book["asks"])

                    # determines cost at each order book level
                    # provides each row (set of price and quantity) as a different input and transposes
                    bids_cost = np.cumsum(cost(*bids.T))


                    asks = np.array(order_book["asks"])
                    print(bids_cost)

                    try:
                        print(order_book["timestamp"] - timestamp_ms)
                    except:
                        pass

                    timestamp_ms = order_book['timestamp']
                    
                    print(f"Timestamp: {timestamp_ms} | Best Bid: {best_bid} | Best Ask: {best_ask}")

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

            # Wait for one second before the next poll
            await asyncio.sleep(1)

    finally:
        # Always close the exchange connection when done
        print("-" * 60)
        print("Closing the connection to the exchange.")
        await exchange.close()

if __name__ == "__main__":
    try:
        #asyncio.run(poll_coinbase_bid_ask())
        
        arr = np.array([
            [1.0086000e+05, 6.7132000e-01], 
            [1.0085809e+05, 7.6210000e-02],
            [1.0085808e+05, 1.2000000e-01],
            [1.0085800e+05, 5.7100000e-02],
            [1.0085800e+05, 5.7100000e-02],
            [1.0085757e+05, 1.9830000e-02],
            [1.0085600e+05, 5.7100000e-02],
            [1.0085448e+05, 2.9740000e-02],
            [1.0085290e+05, 7.9830000e-02],
            [1.0085274e+05, 5.0000000e-02],
            [1.0085266e+05, 9.2740000e-02]

        ])
        print(arr[0,0])
        print(vwap(arr))
    except KeyboardInterrupt:
        print("\nPolling stopped by user.")