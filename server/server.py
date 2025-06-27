from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
import io
import secrets
import datetime
import zipfile

from manager import BackgroundTaskManager
from to_csv import get_data_as_csv_bytes

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
    start_date: datetime.date | int
    end_date: datetime.date | int
    info: list[str, str]

# --- Create the Singleton Manager Instance ---
task_manager = BackgroundTaskManager()

# --- Create the FastAPI App with Lifespan Events ---
app = FastAPI(lifespan=task_manager.lifespan)

@app.on_event("startup")
async def on_startup():
    """Server startup event handler."""
    print("--- Server starting up. Initializing background task. ---")
    # Start the background task with an initial default value
    await task_manager.start(initial_input="default_value")

@app.on_event("shutdown")
async def on_shutdown():
    """Server shutdown event handler."""
    print("--- Server shutting down. Stopping background task. ---")
    await task_manager.stop()


# --- API Endpoints ---
@app.post("/command/restart", dependencies=[Depends(verify_secret_token)])
async def restart_worker(command: RestartCommand):
    """
    Receives a command to restart the worker with a new input.
    """
    await task_manager.restart(new_input=command.new_input)
    return {"message": f"Restart command accepted. Worker now running with input: '{command.new_input}'"}

@app.get("/status")
async def get_worker_status():
    """
    Returns the current status of the background worker.
    """
    return task_manager.get_status()


@app.get("/command/download_csv")
async def download_csv(command: DownloadCommand):
    """
    Receives a command to download data from database into csv.
    """

    symbols_to_download = ["EURUSD", "GBPUSD", "FAKE_SYMBOL"]

    # 1. Create an in-memory binary buffer
    in_memory_zip = io.BytesIO()

    # 2. Create a ZIP file writer on the in-memory buffer
    with zipfile.ZipFile(in_memory_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        # 3. Loop through the desired reports
        for symbol in symbols_to_download:
            print(f"Generating report for {symbol}...")
            csv_data_bytes = get_data_as_csv_bytes(symbol)
            # 4. Write the data to a file inside the zip archive
            zf.writestr(f"{symbol}_report.csv", csv_data_bytes)

    # 5. Rewind the buffer to the beginning
    in_memory_zip.seek(0)

    # 6. Set headers for the ZIP file download
    headers = {
        "Content-Disposition": f"attachment; filename=\"market_data_reports_{pd.Timestamp.now().strftime('%Y%m%d')}.zip\""
    }

    # 7. Stream the in-memory ZIP file to the client
    return StreamingResponse(
        in_memory_zip,
        media_type="application/zip",
        headers=headers
    )