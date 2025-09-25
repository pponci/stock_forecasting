import pandas as pd
import yfinance as yf
import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import get_ticker_list, get_env_var

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


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

    df_csv_paths = pd.read_csv("./storage/other/csv_paths.csv", index_col = 0)

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

        if backup_dir == get_env_var(key = "backup_dir1"):
            df_csv_paths.loc[len(df_csv_paths)] = {"ext_date" : end_date, "ticker" : ticker_name, "path" : file_path}
    
    df_csv_paths.to_csv("./storage/other/csv_paths.csv")


def download_pipeline():

    end_date = datetime.datetime.now().date()
    start_date = end_date + datetime.timedelta(days = -6)

    ticker_list = get_ticker_list()

    for ticker in ticker_list:
        data = download_data(ticker_name = ticker, start_date = start_date, end_date = end_date)

        if not data.empty:
            csv_data_backup(data = data, ticker_name = ticker, end_date = end_date)


if __name__ == "__main__":
    download_pipeline()