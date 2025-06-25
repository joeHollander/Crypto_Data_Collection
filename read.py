from arcticdb import Arctic
import pandas as pd
import numpy as np
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# --- Load Data ---
# It's good practice to wrap file reading in a try/except block
try:
    btc = pd.read_csv("output_csvs/cryptocom_BTC_USD.csv")
    eth = pd.read_csv("output_csvs/cryptocom_ETH_USD.csv")
except FileNotFoundError as e:
    print(f"Error: Could not find a data file. {e}")
    exit() # Exit the script if data can't be loaded

# --- Prepare Data ---
# **THE FIX IS HERE**: Specify unit='ms' to correctly interpret the timestamp
# This tells pandas the integer represents milliseconds since the epoch.
btc["timestamp"] = pd.to_datetime(btc["timestamp"], unit='ms')
eth["timestamp"] = pd.to_datetime(eth["timestamp"], unit='ms')

# Taking a slice of the data for a clearer plot
btc = btc.iloc[100:300, :]
eth = eth.iloc[100:300, :] # Also slicing eth for consistency

print("Sample of prepared BTC data:")
print(btc.head())

# --- Create Plot ---
fig, ax = plt.subplots(2, 1, figsize=(14, 10))
fig.suptitle('Crypto VWAP Analysis', fontsize=16)


# --- Plot BTC Data ---
ax[0].plot(btc["timestamp"], btc["ask_vwap"], color='orange', label='Ask VWAP')
ax[0].set_title("BTC/USD")
ax[0].set_ylabel("Price (USD)")
ax[0].grid(True, linestyle='--', alpha=0.6)
ax[0].legend()

# --- Plot ETH Data ---
# Using a line plot for ETH as well for better time-series visualization
ax[1].plot(eth["timestamp"], eth["ask_vwap"], color='blue', label='Ask VWAP')
ax[1].set_title("ETH/USD")
ax[1].set_ylabel("Price (USD)")
ax[1].grid(True, linestyle='--', alpha=0.6)
ax[1].legend()

# --- Format the Dates ---
# Define the format you want for the date labels on the x-axis
# %H:%M:%S will show Hour:Minute:Second
date_format = mdates.DateFormatter('%H:%M:%S')

# Apply the formatter to both subplots' x-axes
ax[0].xaxis.set_major_formatter(date_format)
ax[1].xaxis.set_major_formatter(date_format)

# This automatically rotates the date labels to fit them nicely
fig.autofmt_xdate(rotation=45)

# Improve layout to prevent titles/labels overlapping
plt.tight_layout(rect=[0, 0.03, 1, 0.95]) # Adjust rect to make space for suptitle

# --- Show the Plot ---
plt.show()
