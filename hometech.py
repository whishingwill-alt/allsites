import requests
import time
from concurrent.futures import ThreadPoolExecutor

# URL to hit
URL = "https://batechdemo.in/hometech/"

# Function to send a single request
def send_request():
    try:
        response = requests.get(URL)
        print(f"Status: {response.status_code}")
    except Exception as e:
        print(f"Error: {e}")

# Main loop: send 100 requests per second indefinitely
if __name__ == "__main__":
    executor = ThreadPoolExecutor(max_workers=100)

    while True:
        start_time = time.time()

        # Submit 100 requests concurrently
        for _ in range(1000):
            executor.submit(send_request)

        # Wait to ensure 1-second pacing
        elapsed = time.time() - start_time
        if elapsed < 1.0:
            time.sleep(1.0 - elapsed)
