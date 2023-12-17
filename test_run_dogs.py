import pandas as pd
import json
import csv
import re # regular expressions, for cleaning text
import geopandas as gpd
import datetime
import numpy as np

all_dogs_df = pd.read_csv("C:/workspace/nyc_dogs_project/csv/all_nyc_dogs.csv", dtype={"zipcode": str}, parse_dates=["licenseissueddate","licenseexpireddate"])

#print(len(all_dogs_df))

print(len(all_dogs_df))
print(len(all_dogs_df.query("licenseexpireddate.dt.year >= 2022 or licenseexpireddate.isna()")))

print(len(all_dogs_df.query("licenseexpireddate.isna()")))

manhattan_zips = [str(zipcode) for zipcode in range(10001,10283)]
staten_zips = [str(zipcode) for zipcode in range(10301,10315)]
bronx_zips = [str(zipcode) for zipcode in range(10451,10476)]
queens_zips = [str(zipcode) for zipcode in range(11004,11110)] + [str(zipcode) for zipcode in range(11351,11698)]
brooklyn_zips = [str(zipcode) for zipcode in range(11201,11257)]
nyc_zips = manhattan_zips + staten_zips + bronx_zips + queens_zips + brooklyn_zips

all_dogs_nyc_df = all_dogs_df[all_dogs_df["zipcode"].isin(nyc_zips)].reset_index(drop=True)

print(len(all_dogs_nyc_df))
print(len(all_dogs_nyc_df.query("licenseexpireddate.dt.year >= 2022 or licenseexpireddate.isna()")))

print(len(all_dogs_nyc_df.query("licenseexpireddate.isna()")))

print(len(all_dogs_df.query("licenseexpireddate.dt.year == 2016 or licenseexpireddate.dt.year == 2017 or licenseexpireddate.dt.year == 2018")))