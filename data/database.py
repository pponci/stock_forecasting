import psycopg2 as pc
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import get_ticker_list, db_connect

def create_schemas():
    """
    Creates the schemas inside the database.
    """
    db = db_connect()
    cur = db.cursor()

    schemas = ["raw_all", "raw_unique", "filled"]

    for schema in schemas:
        sql_str = f"CREATE SCHEMA {schema};"

        cur.execute(sql_str)
        db.commit()
    
    cur.close()
    db.close()


def create_tables_views():
    """
    Creates all the tables and views inside the database.
    """

    db = db_connect()
    cur = db.cursor()

    ticker_list = get_ticker_list()

    for ticker in ticker_list:

        sql_str = f"""
        CREATE TABLE raw_all.tab_{ticker} (
            ext_date DATE,
            ref_date DATE,
            ref_time TIME,
            ref_datetime VARCHAR(25),
            v_open FLOAT,
            v_high FLOAT,
            v_low FLOAT,
            v_close FLOAT,
            v_volume FLOAT,
            PRIMARY KEY(ext_date, ref_date, ref_time)
        );
        """
        cur.execute(sql_str)
        db.commit()

        sql_str = f"""
        CREATE VIEW raw_all.vw_{ticker} AS 
        SELECT 
            RANK() OVER(PARTITION BY ref_date ORDER BY ext_date) AS ext_rank,
            ext_date,
            ref_date,
            ref_time,
            ref_datetime,
            v_open,
            v_high,
            v_low,
            v_close,
            v_volume
        FROM
            raw_all.tab_{ticker};
        """
        cur.execute(sql_str)
        db.commit()

        sql_str = f"""
        CREATE TABLE raw_unique.tab_{ticker} (
            ref_date DATE,
            ref_time TIME,
            ref_datetime VARCHAR(25),
            v_open FLOAT,
            v_high FLOAT,
            v_low FLOAT,
            v_close FLOAT,
            v_volume FLOAT,
            PRIMARY KEY(ref_date, ref_time)
        );
        """
        cur.execute(sql_str)
        db.commit()

        sql_str = f"""
        CREATE VIEW raw_unique.vw_{ticker} AS 
        SELECT 
            *
        FROM
            raw_unique.tab_{ticker};
        """
        cur.execute(sql_str)
        db.commit()

        sql_str = f"""
        CREATE TABLE filled.tab_{ticker}(
            ref_date DATE,
            ref_time TIME,
            ref_datetime VARCHAR(25),
            v_open FLOAT,
            v_high FLOAT,
            v_low FLOAT,
            v_close FLOAT,
            v_volume FLOAT,
            PRIMARY KEY(ref_date, ref_time)
        );
        """
        cur.execute(sql_str)
        db.commit()

        sql_str = f"""
        CREATE VIEW filled.vw_{ticker} AS 
        SELECT
            *
        FROM
            filled.tab_{ticker};
        """
        cur.execute(sql_str)
        db.commit()
    
    cur.close()
    db.close()

if __name__ == "__main__":
    create_schemas()

    create_tables_views()