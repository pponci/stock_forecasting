import psycopg2 as pc
import json
import os

from sqlalchemy import create_engine
from dotenv import load_dotenv

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def get_ticker_list() -> list:
    """
    Fetches the ticker list
    """

    with open("./storage/other/ticker_list.json", "r") as f:
        ticker_list = json.load(f)

    return ticker_list


def get_env_var(key : str):
    """
    Fetches one enviroment variable by key.
    """

    load_dotenv()

    return os.getenv(key)


def db_connect():
    """
    Creates the connection to the database.
    """

    db = pc.connect(
        host = get_env_var("DB_HOST"), 
        dbname = get_env_var("DB_NAME"), 
        user = get_env_var("DB_USER"),
        password = get_env_var("DB_PASSWORD"),
        port = get_env_var("DB_PORT"))
    
    return db


def create_db_engine():
    """
    Creates the engine to use for the df.to_sql function.
    """

    return create_engine(f"postgresql://{get_env_var("DB_USER")}:{get_env_var("DB_PASSWORD")}@{get_env_var("DB_HOST")}:{get_env_var("DB_PORT")}/{get_env_var("DB_NAME")}")