import pandas as pd
import datetime
import glob
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.helpers import create_db_engine, get_env_var

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


if __name__ == "__main__":
    compute_csv_paths()