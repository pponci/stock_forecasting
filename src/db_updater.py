import pandas as pd
import numpy as np
import pandas_market_calendars as mcal
import datetime
import glob
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import create_db_engine, get_env_var, db_connect, get_ticker_list

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def compute_csv_paths():
    """
    Computes all file paths in the backup dir and saves
    a dataframe with the extraction data, ticker, and path.
    """

    df_csv = pd.DataFrame(columns = ["ext_date", "ticker", "path"])

    csv_dir = get_env_var("backup_dir1")

    months = glob.glob(csv_dir + "/*")
    months.sort()

    for month in months:
        days = glob.glob(month + "/*")
        days.sort()

        for day in days:
            ext_date = day[-10:].replace("_", "-")
            
            files = glob.glob(day + "/*")
            files.sort()

            for file in files:
                ticker = file[65:-15]

                df_csv.loc[len(df_csv)] = {"ext_date" : ext_date, "ticker" : ticker, "path" : file}

    df_csv.to_csv("./storage/other/csv_paths.csv")


def get_last_date(schema : str) -> pd.DataFrame:
    """
    Computes a dataframe with the last date present in the db
    for each ticker, checking in the raw all schema.
    """

    if schema == "raw_all":
        target_date = "ext_date"
    elif schema in ["raw_unique", "filled"]:
        target_date = "ref_date"
    else:
        print("invalida schema value")
        return

    db = db_connect()
    cur = db.cursor()

    df_dates = pd.DataFrame(columns = ["ticker", "last_date"])

    ticker_list = get_ticker_list()

    for ticker in ticker_list:
        sql_str = f"""
        SELECT
            MAX({target_date})
        FROM
            {schema}.vw_{ticker};
        """

        cur.execute(sql_str)
        res_sql = cur.fetchall()

        last_date = res_sql[0][0]

        if not last_date:
            last_date = "none"  

        df_dates.loc[len(df_dates)] = {"ticker" : ticker, "last_date" : last_date}


    db.close()
    cur.close()

    return df_dates


def data_to_db(data : pd.DataFrame, ticker_name : str, end_date : datetime.date):
    """
    Saves the data in the database.
    """

    ticker_name = ticker_name.lower()

    data["Datetime"] = data["Datetime"].astype(str)

    data["ref_date"] = data.Datetime.str.split().str[0]
    data["ref_time"] = data.Datetime.str.split().str[1].str.split("-").str[0]
    data["ext_date"] = end_date

    data.rename({"Datetime" : "ref_datetime", "Open" : "v_open", "High" : "v_high", "Low" : "v_low", "Close" : "v_close", "Volume" : "v_volume"}, axis = 1, inplace = True)
    data = data[["ext_date", "ref_date", "ref_time", "ref_datetime", "v_open", "v_high", "v_low", "v_close", "v_volume"]]

    engine = create_db_engine()
    data.to_sql(f"tab_{ticker_name}", con = engine, schema = "raw_all", if_exists = "append", index = False)


def raw_all_upload():
    """
    Based on the last available date in the database for each ticker, the function
    selects the files to upload into the db, in the raw all schema.
    """

    df_csv = pd.read_csv("./storage/other/csv_paths.csv", index_col = 0, parse_dates = ["ext_date"])

    df_last_dates = get_last_date(schema = "raw_all")
    last_dates = df_last_dates.last_date.unique().tolist()

    for last_date in last_dates:
        ticker_list = df_last_dates.loc[df_last_dates.last_date == last_date, "ticker"].tolist()

        if last_date == "none":
            file_paths = df_csv.loc[df_csv.ticker.isin(ticker_list), "path"].tolist()
        
        else:
            file_paths = df_csv.loc[(df_csv.ticker.isin(ticker_list)) & (df_csv.ext_date > np.datetime64(last_date)), "path"].tolist()

        if len(file_paths) > 0 :
            for file_path in file_paths:
                ticker_name = file_path[65:-15]
                ext_date = file_path[-14:-4].replace("_", "-")
                data = pd.read_csv(file_path, index_col = 0)

                if not data.empty:
                    data_to_db(data = data, ticker_name = ticker_name, end_date = ext_date)

def all_to_unique():
    """
    Updates the values in the raw_unique schema adding only
    the dates missing from said schema.
    """

    db = db_connect()
    cur = db.cursor()

    ticker_list = get_ticker_list()

    for ticker in ticker_list:
        sql_str = f"""
        INSERT INTO raw_unique.tab_{ticker}(
            SELECT
                ref_date,
                ref_time,
                ref_datetime,
                v_open,
                v_high,
                v_low,
                v_close,
                v_volume
            FROM
                raw_all.vw_{ticker}
            WHERE
                ext_rank = 1
                    AND
                ref_date > (
                            SELECT
                                COALESCE(MAX(ref_date), '2000-01-01')
                            FROM
                                raw_unique.vw_{ticker})
        );
        """
        cur.execute(sql_str)
        db.commit()

    cur.close()
    db.close()


def unique_to_filled():
    """
    Updates the values in the filled schema adding only
    the dates missing.
    """
    engine = create_db_engine()

    defaul_start_date = datetime.datetime(2023, 11, 26).date()

    df_time = pd.read_csv("./storage/other/time.csv", index_col = 0)

    nasdaq_cal = mcal.get_calendar("NASDAQ")

    df_last_date = get_last_date(schema = "filled")
    last_dates = df_last_date.last_date.unique().tolist()

    for last_date in last_dates:
        ticker_list = df_last_date.loc[df_last_date.last_date == last_date, "ticker"].tolist()

        if last_date == "none":
            last_date = defaul_start_date
        
        open_days_ts = nasdaq_cal.valid_days(
            start_date = last_date + datetime.timedelta(days = + 1), 
            end_date = datetime.datetime.now().date() + datetime.timedelta(days = -1)).tolist()
        open_days = [str(x).split()[0] for x in open_days_ts]
        df_open_days = pd.DataFrame(open_days, columns = ["ref_date"])
        df_open_days = df_open_days.merge(df_time, how = "cross")

        for ticker in ticker_list:
            sql_str = f"""
                        SELECT
                            *
                        FROM
                            raw_unique.vw_{ticker}
                        WHERE
                            ref_date > '{last_date}'
                        ORDER BY
                            ref_datetime
                        ;"""
            
            raw_data = pd.read_sql(sql = sql_str, con = engine)
            raw_data.ref_date = raw_data.ref_date.astype(str)
            raw_data.ref_time = raw_data.ref_time.astype(str)
            raw_data.ref_datetime = raw_data.ref_datetime.str[:-6]

            if not raw_data.empty:
                data = raw_data.merge(df_open_days, on = ["ref_date", "ref_time"], how = "right")

                data.v_volume = data.v_volume.fillna(value = 0.0)
                data.ref_datetime = data.ref_datetime.fillna(data.ref_date + " " + data.ref_time)
                data.ffill(axis = 0, inplace = True)

                data.to_sql(f"tab_{ticker.lower()}", con = engine, schema = "filled", if_exists = "append", index = False)


if __name__ == "__main__":
    print("starting raw_all_upload...")
    raw_all_upload()
    print("done raw_all_upload...")

    print("starting all_to_unique...")
    all_to_unique()
    print("done all_to_unique...")

    print("starting unique_to_filled...")
    unique_to_filled()
    print("done unique_to_filled...")

    print("done")