import pandas as pd
import yfinance as yf
import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import get_ticker_list, get_env_var, create_db_engine


def download_data(ticker_name : str, start_date : datetime.date, end_date : datetime.date) -> pd.DataFrame:
    """
    Dowloads the stock data for the given sticker between the specified dates.
    """

    ticker = yf.Ticker(ticker = ticker_name)

    ticker_data = ticker.history(interval = "1m", start = start_date, end = end_date, actions = False)

    ticker_data.reset_index(drop = False, inplace = True)

    return ticker_data


def csv_data_backup(data : pd.DataFrame, ticker_name : str, end_date : datetime.date):
    """
    Backs up the dowloaded data in csv format in two different drives.
    """

    backup_dirs = [get_env_var(key = "backup_dir1"), get_env_var(key = "backup_dir2")]

    for backup_dir in backup_dirs:

        month_dir = backup_dir + f"/down_{end_date.strftime("%Y_%m")}"

        if not os.path.isdir(month_dir):
            os.makedirs(month_dir)
        

        day_dir = month_dir + f"/down_{end_date.strftime("%Y_%m_%d")}"

        if not os.path.isdir(day_dir):
            os.makedirs(day_dir)
        

        file_path = day_dir + f"/{ticker_name}_{end_date.strftime("%Y_%m_%d")}.csv"

        data.to_csv(file_path)


def data_to_db(data : pd.DataFrame, ticker_name : str, end_date : datetime.date):
    """
    Saves the data in the database.
    """

    data.Datetime = data.Datetime.astype(str)

    data["ref_date"] = data.Datetime.str.split().str[0]
    data["ref_time"] = data.Datetime.str.split().str[1].str.split("-").str[0]
    data["ext_date"] = end_date

    data.rename({"Datetime" : "ref_datetime", "Open" : "v_open", "High" : "v_high", "Low" : "v_low", "Close" : "v_close", "Volume" : "v_volume"}, axis = 1, inplace = True)
    data = data[["ext_date", "ref_date", "ref_time", "ref_datetime", "v_open", "v_high", "v_low", "v_close", "v_volume"]]

    engine = create_db_engine()
    data.to_sql(f"tab_{ticker_name}", con = engine, schema = "raw_all", if_exists = "append", index = False)


def download_pipeline():

    end_date = datetime.datetime.now().date()
    start_date = end_date + datetime.timedelta(days = -6)

    ticker_list = get_ticker_list()

    for ticker in ticker_list:
        data = download_data(ticker_name = ticker, start_date = start_date, end_date = end_date)

        if not data.empty:
            csv_data_backup(data = data, ticker_name = ticker, end_date = end_date)

            data_to_db(data = data, ticker_name = ticker, end_date = end_date)

if __name__ == "__main__":
    download_pipeline()