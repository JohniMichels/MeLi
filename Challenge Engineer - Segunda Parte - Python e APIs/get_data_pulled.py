import asyncio
from typing import Any, AsyncIterator, Dict, List, Tuple
import aiohttp
import aiofiles as aiof
import time

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
search_url = "https://api.mercadolibre.com/sites/MLA/search"
item_url = "https://api.mercadolibre.com/items"


def parse_search_response(
    response: Dict[str, Any]
) -> Tuple[List[Dict[str, Any]], bool, int]:
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
    results: List[Dict[str, Any]] = response["results"]
    should_continue: bool = response["paging"]["total"] > (
        response["paging"]["offset"] + response["paging"]["limit"]
    )
    next_offset: int = response["paging"]["offset"] + response["paging"]["limit"]
    return results, should_continue, next_offset


async def fetch_search(
    session: aiohttp.ClientSession,
    query: str,
    offset: int = 0
) -> AsyncIterator[str]:
    """
    Fetches search results asynchronously.

    Args:
        session (aiohttp.ClientSession): The aiohttp client session.
        query (str): The search query.
        offset (int, optional): The offset for pagination. Defaults to 0.

    Yields:
        Dict[str, Any]: A dictionary representing a search result.

    Returns:
        AsyncIterator[str]: An async iterator that yields the IDs of the search results.
    """
    
    query_params = {
        "q": query,
        "offset": offset,
        **main_filters
    }
    async with session.get(f"{search_url}", params=query_params) as response:
        print(f"Searching for {query} with offset {offset}")
        results, should_continue, next_offset = parse_search_response(await response.json())
        for result in results:
            yield result["id"]
        if should_continue:
            async for result in fetch_search(session, query, next_offset):
                yield result


async def fetch_item(
    session: aiohttp.ClientSession,
    item_id: str
) -> Dict[str, Any]:
    """
    Fetches an item from the server using the provided session and item ID.

    Args:
        session (aiohttp.ClientSession): The client session to use for the request.
        item_id (str): The ID of the item to fetch.

    Returns:
        Dict[str, Any]: A dictionary containing the fetched item.

    """
    async with session.get(f"{item_url}/{item_id}") as response:
        return await response.json()


def get_from_attributes(attributes: List[Dict[str, Any]], key: str) -> str:
    """
    Returns the value associated with the given key from a list of attributes.

    Parameters:
    attributes (list): A list of attribute dictionaries.
    key (str): The key to search for in the attribute dictionaries.

    Returns:
    str or None: The value associated with the given key, or None if the key is not found.
    """
    return next((i["value_name"] for i in attributes if i["id"] == key), None)


def parse_item(item: Dict[str, Any]) -> Dict[str, Any]:
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


def create_csv_line(item: Dict[str, Any]) -> str:
    return ",".join([f"{k[1]}{item[k[0]]}{k[1]}" for k in key_escape]) + "\n"


def create_csv_header() -> str:
    return ",".join([f"\"{k[0]}\"" for k in key_escape]) + "\n"


async def main():
    async with aiohttp.ClientSession() as session, aiof.open("result_pull.csv", "w") as f:
        session._default_headers["User-Agent"] = "PostmanRuntime/7.37.0"
        await f.write(create_csv_header())
    
        for cell in cell_phones_to_check:
            async for item_id in fetch_search(session, cell):
                item = await fetch_item(session, item_id)
                parsed_item = parse_item(item)
                csv_line = create_csv_line(parsed_item)
                print(csv_line)
                await f.write(csv_line)


start_time = time.perf_counter()
asyncio.run(main())
end_time = time.perf_counter()
print(f"Finished pull-based in {end_time - start_time} seconds.")
