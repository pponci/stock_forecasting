import psycopg2 as pc
import json
import os

from sqlalchemy import create_engine

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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


def db_connect():
    """
    Creates the connection to the database.
    """

    db = pc.connect(
        host = get_env_var("db_host"), 
        dbname = get_env_var("db_name"), 
        user = get_env_var("db_user"),
        password = get_env_var("db_password"))
    
    return db


def create_db_engine():
    """
    Creates the engine to use for the df.to_sql function.
    """

    return create_engine(f"postgresql://{get_env_var("db_user")}:{get_env_var("db_password")}@{get_env_var("db_host")}:5432/{get_env_var("db_name")}")