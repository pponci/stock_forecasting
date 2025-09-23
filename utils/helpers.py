import json

def get_ticker_list() -> list:
    """
    Fetches the ticker list
    """

    with open("./storage/other/ticker_list.json", "r") as f:
        ticker_list = json.load(f)

    return ticker_list


def get_env_vars() -> dict:
    """
    Fetches the enviroment variables
    """

    with open("./storage/other/env_variables.json", "r") as f:
        env_vars = json.load(f)
    
    return env_vars


def get_env_var(key : str):
    """
    Fetches one enviroment variable by key.
    """

    env_vars = get_env_vars()

    return env_vars[key]