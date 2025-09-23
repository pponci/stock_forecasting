import json

def get_ticker_list() -> list:
    """
    Fetches the ticker list
    """

    with open("./storage/tickers/ticker_list.json", "r") as f:
        ticker_list = json.load(f)

    return ticker_list