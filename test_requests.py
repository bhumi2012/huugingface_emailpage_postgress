import requests
import time
import re

URL = "http://localhost:8000/reviews"
TOTAL_REQUESTS = 10

completed = 0

print("Sending requests with interactive countdown when blocked...\n")

while completed < TOTAL_REQUESTS:

    r = requests.get(URL)

    if r.status_code == 200:
        completed += 1
        print(f"Request {completed}: Success")

    elif r.status_code == 429:

        detail = r.json().get("detail", "")
        print(f"\nRate limited: {detail}")

        # extract seconds
        match = re.search(r"(\d+)", detail)
        wait_time = int(match.group(1)) if match else 10

        print("Waiting...")

        for sec in range(wait_time, 0, -1):
            print(f"{sec} ", end="\r")
            time.sleep(1)

        print("Retrying...\n")

    else:
        print("Unexpected response:", r.status_code)
        break

print("\nAll requests completed successfully.")
