import downloader as dowloader
import db_updater as db_updater
import os

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

dowloader.download_pipeline()

db_updater.raw_all_upload()

db_updater.all_to_unique()

db_updater.unique_to_filled()