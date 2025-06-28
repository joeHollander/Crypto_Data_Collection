from fastapi import FastAPI, Depends, HTTPException, Header, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from arcticdb import Arctic, QueryBuilder
from contextlib import asynccontextmanager
from typing import Optional, AsyncGenerator
import datetime as dt
import pandas as pd
import io
import secrets
import datetime
import zipfile



from manager import TaskManager

# TODO: secret key env variable, test server on pc, docker full server, 

# --- Configuration & Security ---
SECRET_TOKEN = "my-control-secret-12345" # Replace with a proper secret

async def verify_secret_token(x_secret_token: str = Header(...)):
    if not secrets.compare_digest(x_secret_token, SECRET_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid secret token")

# --- Pydantic Model for our command ---
class RestartCommand(BaseModel):
    new_input: list[tuple[str, str, int]]

class DownloadCommand(BaseModel):
    start_date: int
    end_date: int
    info: list[str, str, int]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """
    The lifespan context manager.
    """
    # --- Code to run on startup ---
    print("--- Application Startup ---")
    # Create a single instance of the manager
    task_manager = TaskManager()
    
    # Store the manager instance in the application state
    # This makes it accessible from your endpoints
    app.state.task_manager = task_manager
    
    # Define your initial inputs here
    initial_task_inputs = [("cryptocom", "BTC/USD", 1), ("cryptocom", "ETH/USD", 1)]
    await task_manager.start(initial_task_inputs)

    yield # The application is now running

    # --- Code to run on shutdown ---
    print("--- Application Shutdown ---")
    await task_manager.stop()

# --- Create the FastAPI App with Lifespan Events ---
app = FastAPI(lifespan=lifespan)

# --- API Endpoints ---
@app.post("/command/restart", dependencies=[Depends(verify_secret_token)])
async def restart_worker(command: RestartCommand, request: Request):
    """
    Receives a command to restart the worker with a new input.
    It now gets the task_manager from the request state.
    """
    manager = request.app.state.task_manager
    await manager.restart(new_input=command.new_input)
    return {"message": f"Restart command accepted. Worker now running with input: '{command.new_input}'"}

@app.get("/status")
async def get_worker_status(request: Request):
    """
    Returns the current status of the background worker by accessing
    the manager from the application state.
    """
    manager = request.app.state.task_manager
    return manager.get_status()

def get_filtered_data_as_csv_bytes(symbol: str, start_ms: int, end_ms: int) -> bytes:
    """
    Queries a symbol for a specific millisecond range, and returns the
    resulting DataFrame as CSV bytes.
    
    This helper function encapsulates the query logic.
    """
    print(f"Querying '{symbol}' from {start_ms} to {end_ms}...")

    lmdb_uri = "lmdb://crypto_data"
    ac = Arctic(lmdb_uri)
    lib = ac.get_library("rest_data_test_concurrent", create_if_missing=True)

    try:
        q = QueryBuilder()
        # This filter works on the 'unix_timestamp_ms' column
        # Note: You would need a different filter for data indexed by timestamp (see EURUSD case)

        filter = (q.timestamp >= start_ms) & (q.unix_timestamp_ms <= end_ms)
        df = lib.read(symbol, filter=filter).data
        
        if df.empty:
            return "No data found for the given date range.".encode('utf-8')
        
        # Convert the resulting DataFrame to a CSV string in memory
        string_buffer = io.StringIO()
        df.to_csv(string_buffer, index=True)
        return string_buffer.getvalue().encode('utf-8')

    except Exception as e:
        print(f"An error occurred while fetching data for {symbol}: {e}")
        return f"Error processing data for symbol '{symbol}'.".encode('utf-8')



@app.get("/command/download_csv")
async def download_csv(command: DownloadCommand):
    """
    Receives a command to download data from database into csv.
    """

    # 1. Create an in-memory binary buffer
    in_memory_zip = io.BytesIO()

    # 2. Create a ZIP file writer on the in-memory buffer
    with zipfile.ZipFile(in_memory_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # 3. Loop through the desired reports
        for i in command.info:
            symbol = f"{i[0]}_{i[1]}_{i[2]}s"
            print(f"Generating report for {symbol}...")
            csv_data_bytes = get_filtered_data_as_csv_bytes(symbol, command.start_date, command.end_date)
            # 4. Write the data to a file inside the zip archive
            start_format = dt.datetime.fromtimestamp(command.start_date).strftime("%Y_%m_%d")
            end_format = dt.datetime.fromtimestamp(command.end_date).strftime("%Y_%m_%d")
            fname = f"{symbol}_{start_format}_{end_format}"
            zf.writestr(f"{fname}.csv", csv_data_bytes)

    # 5. Rewind the buffer to the beginning
    in_memory_zip.seek(0)

    # 6. Set headers for the ZIP file download
    headers = {
        "Content-Disposition": f"attachment; filename=\"crypto_data_{pd.Timestamp.now().strftime('%Y%m%d')}.zip\""
    }

    # 7. Stream the in-memory ZIP file to the client
    return StreamingResponse(
        in_memory_zip,
        media_type="application/zip",
        headers=headers
    )