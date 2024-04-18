import asyncio
import time
import reactivex as rx
import reactivex.operators as ops
import requests
import json
import pandas as pd
from urllib.parse import urlencode
import aiohttp
import requests.adapters

cell_phones_to_check = [
    "samsung",
    "iphone",
    "xiaomi",
    "motorola"
]
main_filters = {
    "category": "MLA1055",
    "price": "200000-10000000",
    "RAM": "[8GB-*)",
    "INTERNAL_MEMORY": "(64GB-*)",
    "limit": 50
}

requests.adapters.DEFAULT_RETRIES = 5

loop = asyncio.get_event_loop()


def fetch_get(url):
    """
    Fetches data from the specified URL using a GET request.

    Parameters:
    url (str): The URL to fetch data from.

    Returns:
    dict: The JSON response from the GET request.

    """
    return loop.run_in_executor(None, lambda: requests.get(url, timeout=10).json())


def parse_search_response(response):
    """
    Parses the search response and extracts the necessary information.

    Args:
        response (dict): The response received from the search API.

    Returns:
        tuple: A tuple containing the following elements:
            - results (list[dict]): A list of dictionaries representing the search results.
            - should_continue (bool): A boolean indicating whether there are more results to fetch.
            - next_offset (int): The offset value for the next API call.

    """
    if "results" not in response:
        return [], False, 0
    results: list[dict] = response["results"]
    should_continue: bool = response["paging"]["total"] > (
        response["paging"]["offset"] + response["paging"]["limit"]
    )
    next_offset: int = response["paging"]["offset"] + response["paging"]["limit"]
    return results, should_continue, next_offset


def fetch_search(query: str, offset: int = 0):
    """
    Fetches search results from the MercadoLibre API based on the given query and offset.

    Args:
        query (str): The search query.
        offset (int, optional): The offset value for pagination. Defaults to 0.

    Returns:
        rx.Observable: An RxPy Observable that emits the search results.

    """
    query_params = {
        "q": query,
        "offset": offset,
        **main_filters
    }
    query_string = urlencode(query_params)
    print(f"Searching for {query} with offset {offset}")
    url = f"https://api.mercadolibre.com/sites/MLA/search?{query_string}"
    return rx.from_([url]).pipe(
        ops.flat_map(lambda u: fetch_get(u)),
        ops.map(lambda r: parse_search_response(r)),
        ops.flat_map(
            lambda r: rx.from_(r[0]).pipe(
                ops.concat(fetch_search(query, r[2]) if r[1] else rx.empty())
            )
        )
    )


def fetch_item(item_id: str):
    """
    Fetches an item from the MercadoLibre API based on the provided item ID.

    Parameters:
    item_id (str): The ID of the item to fetch.

    Returns:
    The response from the API call.

    """
    url = f"https://api.mercadolibre.com/items/{item_id}"
    print(f"Fetching item {item_id}")
    return fetch_get(url)


def get_from_attributes(attributes, key):
    """
    Returns the value associated with the given key from a list of attributes.

    Parameters:
    attributes (list): A list of attribute dictionaries.
    key (str): The key to search for in the attribute dictionaries.

    Returns:
    str or None: The value associated with the given key, or None if the key is not found.
    """
    return next((i["value_name"] for i in attributes if i["id"] == key), None)


def parse_item(item):
    """
    Parses the given item and returns a dictionary containing the relevant information.

    Args:
        item (dict): The item to be parsed.

    Returns:
        dict: A dictionary containing the parsed information with the following keys:
            - "id": The ID of the item.
            - "title": The title of the item.
            - "price": The price of the item.
            - "permalink": The permalink of the item.
            - "brand": The brand of the item.
            - "line": The line of the item.
            - "model": The model of the item.
            - "processor": The processor of the item.
            - "storage": The storage of the item.
            - "memory": The memory of the item.
    """
    return {
        "id": item["id"],
        "title": item["title"],
        "price": item["price"],
        "permalink": item["permalink"],
        "brand": get_from_attributes(item["attributes"], "BRAND"),
        "line": get_from_attributes(item["attributes"], "LINE"),
        "model": get_from_attributes(item["attributes"], "MODEL"),
        "processor": (
            get_from_attributes(item["attributes"], "PROCESSOR_MODEL")
            or get_from_attributes(item["attributes"], "CPU_MODEL")
        ),
        "storage": get_from_attributes(item["attributes"], "INTERNAL_MEMORY"),
        "memory": get_from_attributes(item["attributes"], "RAM"),
        "condition": item["condition"]
    }


def log_and_return(x):
    """
    Logs the input value and returns it.

    Parameters:
    x (any): The value to be logged and returned.

    Returns:
    any: The input value.

    """
    print(x)
    return x


key_escape = [
    ("id", "\""),
    ("title", "\""),
    ("price", ""),
    ("permalink", "\""),
    ("brand", "\""),
    ("line", "\""),
    ("model", "\""),
    ("processor", "\""),
    ("storage", "\""),
    ("memory", "\""),
    ("condition", "\""),
]


def create_csv_header() -> str:
    return ",".join([f"\"{k[0]}\"" for k in key_escape]) + "\n"


def create_csv_line(item):
    return ",".join([f"{k[1]}{item[k[0]]}{k[1]}" for k in key_escape]) + "\n"


with open("result_push.csv", "w") as f:
    f.write(create_csv_header())
    obs = rx.from_(cell_phones_to_check).pipe(
        ops.flat_map(lambda q: fetch_search(q)),
        ops.map(lambda i: i["id"]),
        ops.flat_map(fetch_item),
        ops.map(parse_item),
        ops.map(create_csv_line),
        ops.map(log_and_return),
        ops.map(lambda i: f.write(i))
    )
    start_time = time.perf_counter()
    loop.run_until_complete(obs)
    end_time = time.perf_counter()
    print(f"Finished push-based in {end_time - start_time} seconds.")
