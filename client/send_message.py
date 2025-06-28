import requests
import os

url = "http://127.0.0.1:8000/command/restart"
#SERVER_KEY = os.environ.get("SERVER_KEY")
SERVER_KEY = "my-control-secret-12345"

headers = {
    "Content-Type": "application/json",
    "x-secret-token": SERVER_KEY
}

payload = {
    "new_input": [("cryptocom", "BTC/USD", 2)]
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

