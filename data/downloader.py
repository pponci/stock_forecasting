import pandas as pd
import yfinance as yf
import datetime
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import get_ticker_list


def download_data(ticker_name : str, start_date : datetime.date, end_date : datetime.date) -> pd.DataFrame:
    """
    Dowloads the stock data for the given sticker between the specified dates
    """

    ticker = yf.Ticker(ticker = ticker_name)

    ticker_data = ticker.history(interval = "1m", start = start_date, end = end_date, actions = False)

    ticker_data.reset_index(drop = False, inplace = True)

    return ticker_data


