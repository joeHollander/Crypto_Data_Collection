import asyncio
import ccxt.async_support as ccxt
from datetime import datetime as dt

async def print_poloniex_ethbtc_ticker():
    poloniex = ccxt.poloniex()
    packet = await poloniex.fetch_ohlcv('ETH/BTC', params={"interval": "MINUTE_1"})
    await poloniex.close()
    print(packet)

def packet_func(packet):
    info = packet["info"]
    start_time = int(info["startTime"])
    close_time = int(info["closeTime"])
    interval = close_time - start_time 
    print(interval / 1e3)
    print(start_time)

asyncio.run(print_poloniex_ethbtc_ticker())
