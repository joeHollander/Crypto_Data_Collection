from fastapi import FastAPI, Depends, HTTPException, Header
from pydantic import BaseModel
import secrets

from manager import BackgroundTaskManager

# --- Configuration & Security ---
SECRET_TOKEN = "my-control-secret-12345" # Replace with a proper secret

async def verify_secret_token(x_secret_token: str = Header(...)):
    if not secrets.compare_digest(x_secret_token, SECRET_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid secret token")

# --- Pydantic Model for our command ---
class RestartCommand(BaseModel):
    new_input: str

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