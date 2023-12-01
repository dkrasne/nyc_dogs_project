from sodapy import Socrata
import pandas as pd
import requests
import json
import csv
import numpy as np
import datetime as dt

manhattan_zips = [str(zipcode) for zipcode in list(range(10001,10283))]
staten_zips = [str(zipcode) for zipcode in list(range(10301,10315))]
bronx_zips = [str(zipcode) for zipcode in list(range(10451,10476))]
queens_zips = [str(zipcode) for zipcode in list(range(11004,11110))] + [str(zipcode) for zipcode in list(range(11351,11698))]
brooklyn_zips = [str(zipcode) for zipcode in list(range(11201,11257))]
nyc_zips = manhattan_zips + staten_zips + bronx_zips + queens_zips + brooklyn_zips

all_dogs_df = pd.read_csv("C:/workspace/nyc_dogs_project/all_nyc_dogs.csv", dtype={"zipcode": str}, parse_dates=["licenseissueddate","licenseexpireddate"]) # NEED TO IMPORT ZIPCODE AS A STRING

all_dogs_nyc_df = all_dogs_df[all_dogs_df["zipcode"].isin(nyc_zips)].reset_index(drop=True)
all_dogs_nyc_df.animalname = all_dogs_nyc_df.animalname.fillna("NOT PROVIDED")
all_dogs_nyc_df.animalgender = all_dogs_nyc_df.animalgender.fillna("-")
#print(all_dogs_nyc_df[all_dogs_nyc_df.isna().any(axis=1)]) ##prints all rows with null value

# print("Number of dogs in dataset, by extract year:")
# print(all_dogs_nyc_df.groupby("extract_year").size())
# print("Number of absolute duplicates:", sum(all_dogs_nyc_df.duplicated()))

orig_columns = ["animalname","animalgender","animalbirth","breedname","zipcode","licenseissueddate","licenseexpireddate","extract_year"]

## Overwrite df with df grouped by all columns, by size (which turns out to be max 3 in any group; this is reasonable for duplicated extraction); drop size column.
all_dogs_nyc_df = all_dogs_nyc_df.groupby(orig_columns, dropna=False).size().reset_index().rename(columns={0: "size"})
all_dogs_nyc_df.drop(columns="size",inplace=True)

all_dogs_nyc_df["licenseissueyear"] = all_dogs_nyc_df["licenseissueddate"].dt.year
all_dogs_nyc_df["licenseexpireyear"] = all_dogs_nyc_df["licenseexpireddate"].dt.year

all_dogs_nyc_df["dupe"] = all_dogs_nyc_df.duplicated(subset=["animalname","animalgender","animalbirth","breedname","zipcode"])
all_dogs_nyc_df = all_dogs_nyc_df.reset_index().rename(columns={"index": "old_index"})

all_dogs_nyc_df["lxmonth"] = all_dogs_nyc_df.licenseexpireddate.dt.month
all_dogs_nyc_df["lxday"] = all_dogs_nyc_df.licenseexpireddate.dt.day

repeat_index_list = []
for rownum in range(len(all_dogs_nyc_df["old_index"])):
    if (all_dogs_nyc_df.dupe.iloc[rownum] == True) and ((all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]) or (all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]+1)or (all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]-1)):
        repeat_index_list.append(rownum)
    elif(all_dogs_nyc_df.dupe.iloc[rownum] == True) and (((all_dogs_nyc_df.lxmonth.iloc[rownum] == all_dogs_nyc_df.lxmonth.iloc[rownum-1]) and (all_dogs_nyc_df.lxday.iloc[rownum] == all_dogs_nyc_df.lxday.iloc[rownum-1])) or ((all_dogs_nyc_df.lxmonth.iloc[rownum] == all_dogs_nyc_df.lxmonth.iloc[rownum-2]) and (all_dogs_nyc_df.lxday.iloc[rownum] == all_dogs_nyc_df.lxday.iloc[rownum-2]))):
        repeat_index_list.append(rownum)
    else:
        repeat_index_list.append(None)

true_dupe = pd.Series(repeat_index_list)
all_dogs_nyc_df["true_dupe"] = true_dupe.apply(lambda x: "" if pd.isnull(x) else "dupe")

all_dogs_nyc_clean = all_dogs_nyc_df[all_dogs_nyc_df.true_dupe != "dupe"][orig_columns].reset_index(drop=True)
### If all_dogs_nyc_clean ever throws a SettingWithCopyWarning, I may need to add .copy().

all_dogs_num = len(all_dogs_nyc_clean)

all_dogs_nyc_by_breed = all_dogs_nyc_clean.groupby("breedname").size().reset_index().rename(columns={0: "num_breed"})
all_dogs_nyc_by_breed["breed_pct"] = all_dogs_nyc_by_breed.num_breed / all_dogs_num
#print(all_dogs_nyc_by_breed.sort_values("breed_pct",ascending=False))
## Drop any dog that only occurs once in the whole city.
all_dogs_nyc_by_breed = all_dogs_nyc_by_breed[all_dogs_nyc_by_breed.num_breed > 1]
print(all_dogs_nyc_by_breed.sort_values("breed_pct",ascending=False))

#print("Number of dogs remaining in dataset:", len(all_dogs_nyc_clean))
#print("Number of dogs per zipcode:")
all_dogs_nyc_by_zipcode = all_dogs_nyc_clean.groupby("zipcode").size().reset_index().rename(columns={0: "num_dogs"})

#print("Breed by zipcode")
all_dogs_breed_by_zipcode = all_dogs_nyc_clean.groupby(["zipcode","breedname"]).size().reset_index().rename(columns={0: "num_breed"})

all_dogs_breed_by_zipcode["breed_pct"] = all_dogs_breed_by_zipcode.num_breed / all_dogs_breed_by_zipcode.merge(all_dogs_nyc_by_zipcode,how="left")["num_dogs"]
#print(all_dogs_breed_by_zipcode[all_dogs_breed_by_zipcode.breed_pct < 1].sort_values("breed_pct",ascending=False))
#print(all_dogs_breed_by_zipcode)

# print("Breed by zipcode, except Unknown:")
# print(all_dogs_nyc_clean[all_dogs_nyc_clean["breedname"] != "Unknown"].groupby(["zipcode","breedname"]).size().sort_values(ascending = False))

# print("New dogs per year:")
# print(all_dogs_nyc_clean.groupby("extract_year").size())
