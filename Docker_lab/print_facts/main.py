import requests
import time
import os

# The URL for the API is not hard-coded into the application.
# Instead, it is specified at runtime via an environment variable.
base_url = os.environ["BASE_URL"]


def retrieve_and_print_fact():
    try:
        response = requests.get(f"{base_url}/fact")
        response.raise_for_status()
        fact = response.json()
        print(f"Fact: {fact}")
    except requests.exceptions.RequestException as e:
        print(f"Error retrieving fact: {e}")


def main():
    while True:
        retrieve_and_print_fact()
        time.sleep(1)


if __name__ == "__main__":
    main()