import requests
import os

url = "http://127.0.0.1:8000/command"
#SERVER_KEY = os.environ.get("SERVER_KEY")
SERVER_KEY = "crypto2025"
print(SERVER_KEY)

headers = {
    "Content-Type": "application/json",
    "x-secret-token": SERVER_KEY
}

payload = {
    "command": "toggle_feature",
    "enable_feature": False
}

try:
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    data = response.json()

    print(data)

except requests.exceptions.HTTPError as e:
    print(f"An HTTP error occurred: {e}")
    print("--- Server's Error Response ---")
    # You can often get more detail from the response body, even on an error.
    try:
        print(e.response.json())
    except ValueError:
        print(e.response.text) # Print as text if it's not JSON.

