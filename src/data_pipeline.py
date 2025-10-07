import downloader as dowloader
import db_updater as db_updater
import os

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if not os.path.exists("./storage/other/csv_paths.csv"):
    db_updater.compute_csv_paths()

dowloader.downloader()

db_updater.raw_all_upload()

db_updater.all_to_unique()

db_updater.unique_to_filled()