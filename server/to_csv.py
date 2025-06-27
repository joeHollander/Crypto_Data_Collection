import os
import pandas as pd
from arcticdb import Arctic

# --- Configuration ---
# The path to the directory containing your data.mdb file.
# Assumes the script is in a parent folder to 'crypto_data'.
DB_PATH = "lmdb://crypto_data"
# The name of the library you created in your collector script.
LIBRARY_NAME = "rest_data"
# The folder where the CSV files will be saved.
OUTPUT_DIR = "output_csvs"

def export_arctic_to_csv(db_uri, library_name, output_dir):
    """
    Connects to an ArcticDB LMDB instance, reads all symbols from a library,
    and exports each symbol's data to a separate CSV file.
    """
    print(f"Connecting to ArcticDB at: {db_uri}")
    try:
        ac = Arctic(db_uri)
        lib = ac.get_library(library_name)
    except Exception as e:
        print(f"Error connecting to the database or library: {e}")
        print("Please ensure the DB_PATH and LIBRARY_NAME are correct.")
        return

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    print(f"Output directory '{output_dir}' is ready.")

    symbols = lib.list_symbols()

    if not symbols:
        print("No symbols found in the library. Nothing to export.")
        return

    print(f"Found {len(symbols)} symbols. Starting export...")

    for symbol in symbols:
        try:
            print(f"  - Reading symbol: '{symbol}'")
            # Read the full data for the symbol
            data_item = lib.read(symbol)
            df = data_item.data

            if not isinstance(df, pd.DataFrame):
                print(f"    - Warning: Data for symbol '{symbol}' is not a DataFrame. Skipping.")
                continue

            # Construct the output CSV file path
            csv_filename = os.path.join(output_dir, f"{symbol}.csv")
            
            # Export the DataFrame to CSV
            df.to_csv(csv_filename)
            print(f"    - Successfully exported to '{csv_filename}'")

        except Exception as e:
            print(f"    - Error processing symbol '{symbol}': {e}")
    
    print("\nExport process finished.")

if __name__ == "__main__":
    export_arctic_to_csv(DB_PATH, LIBRARY_NAME, OUTPUT_DIR)
