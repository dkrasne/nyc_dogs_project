import os
from sodapy import Socrata  # for getting NYC Open Data
import pandas as pd
import requests
import json
import csv

nyc_data_url = 'data.cityofnewyork.us'
nyc_data_set = "nu7n-tubp"
nyc_app_token = os.environ["NYC_DATA_APP_TOKEN"]
client = Socrata(nyc_data_url,nyc_app_token)

client.timeout = 60

all_dogs_dict = client.get_all(nyc_data_set)
all_dogs_df = pd.DataFrame.from_records(all_dogs_dict)

all_dogs_df.to_csv("C:/workspace/nyc_dogs_project/csv/all_nyc_dogs.csv",index=False)