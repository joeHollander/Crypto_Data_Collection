import secrets
from fastapi import FastAPI, Header, HTTPException, status, Depends
from pydantic import BaseModel
import os

# --- Configuration ---
# In a real application, get this from an environment variable or a secure vault
# For demonstration purposes, we'll define it here.
# To generate a secure secret, run: openssl rand -hex 32
#SERVER_KEY = os.environ.get("SERVER_KEY", "your-super-secret-token")
SERVER_KEY = "crypto2025"

# --- Application State ---
# A simple in-memory state to demonstrate functionality change
app_state = {"feature_enabled": False}

# --- FastAPI App ---
app = FastAPI()

# --- Pydantic Model for the Request Body ---
class CommandPayload(BaseModel):
    command: str
    enable_feature: bool

# --- Dependency for Secret Token Verification ---
async def verify_secret_token(x_secret_token: str = Header(...)):
    """
    Dependency to verify the secret token in the request header.
    """
    if not secrets.compare_digest(x_secret_token, SERVER_KEY):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid secret token",
        )

# --- The Secured Endpoint ---
@app.post("/command", dependencies=[Depends(verify_secret_token)])
async def execute_command(payload: CommandPayload):
    """
    Receives a command and alters server functionality based on the payload.
    This endpoint is protected by the verify_secret_token dependency.
    """
    if payload.command == "toggle_feature":
        app_state["feature_enabled"] = payload.enable_feature
        return {"message": f"Feature has been {'enabled' if payload.enable_feature else 'disabled'}"}
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid command",
        )

@app.get("/feature_status")
async def get_feature_status():
    """
    An endpoint to check the current status of the feature.
    """
    return {"feature_enabled": app_state["feature_enabled"]}

if __name__ == "__main__":
    import uvicorn
    # It's recommended to set the secret token as an environment variable
    # for better security.
    # export SERVER_KEY=$(openssl rand -hex 32)
    print(f"Starting server. Current SERVER_KEY is: {SERVER_KEY}")
    uvicorn.run(app, host="0.0.0.0", port=8000)