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

all_dogs_df = pd.read_csv("C:\workspace/nyc_dogs_project/all_nyc_dogs.csv", dtype={"zipcode": str}, parse_dates=["licenseissueddate","licenseexpireddate"]) # NEED TO IMPORT ZIPCODE AS A STRING
#print(all_dogs_df.info())
all_dogs_nyc_df = all_dogs_df[all_dogs_df["zipcode"].isin(nyc_zips)].reset_index(drop=True)
all_dogs_nyc_df.animalname = all_dogs_nyc_df.animalname.fillna("NOT PROVIDED")
all_dogs_nyc_df.animalgender = all_dogs_nyc_df.animalgender.fillna("-")
#print(all_dogs_nyc_df.info())
#print(all_dogs_nyc_df[all_dogs_nyc_df.isna().any(axis=1)]) ##prints all rows with null value

orig_columns = ["animalname","animalgender","animalbirth","breedname","zipcode","licenseissueddate","licenseexpireddate","extract_year"]

#print(all_dogs_nyc_df.groupby(["zipcode","breedname","animalname","animalgender","animalbirth"]).size().sort_values(ascending=False))

all_dogs_nyc_df = all_dogs_nyc_df.groupby(orig_columns, dropna=False).size().reset_index().rename(columns={0: "size"})
#print(max(all_dogs_nyc_df["size"]))
#print(all_dogs_nyc_df["size"].sum())
all_dogs_nyc_df.drop(columns="size",inplace=True)
#print(all_dogs_nyc_df.info())

all_dogs_nyc_df["licenseissueyear"] = all_dogs_nyc_df["licenseissueddate"].dt.year
all_dogs_nyc_df["licenseexpireyear"] = all_dogs_nyc_df["licenseexpireddate"].dt.year

#print(all_dogs_nyc_df.groupby("licenseexpireyear",dropna=False)["size"].count())
#print(all_dogs_nyc_df.head())
#print(all_dogs_nyc_df.info())

#print(all_dogs_nyc_df.groupby(["animalname","animalgender","animalbirth","breedname","zipcode"]).size().sort_values(ascending=False))


#print(all_dogs_nyc_df[all_dogs_nyc_df["dupe"] == True])
#all_dogs_nyc_df["dupe"] = [True if all_dogs_nyc_df.dupe1 == True and all_dogs_nyc_df.dupe2 == False else False]
#print(all_dogs_nyc_df.head(20))
#print("Number of entries in dataset:",len(all_dogs_nyc_df))
#print("Number of (seemingly) duplicate entries in dataset:",len(all_dogs_nyc_df)-len(all_dogs_nyc_df.drop_duplicates(subset=["animalname","animalgender","animalbirth","breedname","zipcode"])))

#print(all_dogs_nyc_df.groupby(["zipcode","breedname","animalname","animalgender","animalbirth"]).size().sort_values(ascending=False))
#print(all_dogs_nyc_df[(all_dogs_nyc_df["breedname"] == "French Bulldog") & (all_dogs_nyc_df["animalbirth"] == 2020)].groupby(["zipcode","breedname","animalname","animalgender","animalbirth"]).size().sort_values(ascending=False))

all_dogs_nyc_df["dupe"] = all_dogs_nyc_df.duplicated(subset=["animalname","animalgender","animalbirth","breedname","zipcode"])
all_dogs_nyc_df = all_dogs_nyc_df.reset_index().rename(columns={"index": "old_index"})

all_dogs_nyc_df["lxmonth"] = all_dogs_nyc_df.licenseexpireddate.dt.month
all_dogs_nyc_df["lxday"] = all_dogs_nyc_df.licenseexpireddate.dt.day

repeat_index_list = []
for rownum in range(len(all_dogs_nyc_df["old_index"])):
    if (all_dogs_nyc_df.dupe.iloc[rownum] == True) and ((all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]) or (all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]+1)or (all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]-1)):
        #repeat_index_list.append([rownum, all_dogs_repeated_order.old_index.iloc[rownum]])
        repeat_index_list.append(rownum)
    elif(all_dogs_nyc_df.dupe.iloc[rownum] == True) and (((all_dogs_nyc_df.lxmonth.iloc[rownum] == all_dogs_nyc_df.lxmonth.iloc[rownum-1]) and (all_dogs_nyc_df.lxday.iloc[rownum] == all_dogs_nyc_df.lxday.iloc[rownum-1])) or ((all_dogs_nyc_df.lxmonth.iloc[rownum] == all_dogs_nyc_df.lxmonth.iloc[rownum-2]) and (all_dogs_nyc_df.lxday.iloc[rownum] == all_dogs_nyc_df.lxday.iloc[rownum-2]))):
        repeat_index_list.append(rownum)
    else:
        repeat_index_list.append(None)

true_dupe = pd.Series(repeat_index_list)
all_dogs_nyc_df["true_dupe"] = true_dupe.apply(lambda x: "" if pd.isnull(x) else "dupe")
#print(all_dogs_nyc_df)

# Using ZORRO as a test case, the "true_dupe" column seems fairly accurate.
#print(all_dogs_nyc_df[all_dogs_nyc_df.animalname == "ZORRO"].groupby(["zipcode","breedname","animalname","animalgender","animalbirth"]).size().sort_values(ascending=False))
#print(all_dogs_nyc_df[all_dogs_nyc_df.animalname == "ZORRO"].sort_values(by=["animalbirth","zipcode","breedname","animalgender"])[["zipcode","breedname","animalgender","animalbirth","licenseissueddate","licenseexpireddate","dupe","true_dupe"]].tail(50))

#print(all_dogs_nyc_df[all_dogs_nyc_df.animalname == "MAX"].sort_values(by=["animalbirth","zipcode","breedname","animalgender"])[["zipcode","breedname","animalgender","animalbirth","licenseissueddate","licenseexpireddate","dupe","true_dupe"]].tail(50))

#print(all_dogs_repeated_order.head())
#print(all_dogs_repeated_order[(all_dogs_repeated_order.dupe == True) & (all_dogs_repeated_order.true_dupe == "")].groupby(["zipcode","breedname","animalname","animalgender","animalbirth"]).size().sort_values(ascending=False))

# print(all_dogs_nyc_df[(all_dogs_nyc_df.dupe == True) & (all_dogs_nyc_df.true_dupe == "")].groupby(["zipcode","breedname","animalname","animalgender","animalbirth"]).size().sort_values(ascending=False))
#print(all_dogs_nyc_df[(all_dogs_nyc_df.dupe == True) & (all_dogs_nyc_df.true_dupe == "")][["zipcode","breedname","animalname","animalgender","animalbirth","licenseissueddate","licenseexpireddate","extract_year"]].sort_values(by=["animalname","breedname","animalbirth"]))

### once I have unique IDs for the first dog in a repeated group, and Null for the rest, I can use .ffill() to downfill the ID. I could just use old_index as an ID, but there may be some way to generate it as well.
### I still need to figure out what to do with the dogs that are .dupe == True but .true_dupe == "".

#print(all_dogs_nyc_df[(all_dogs_nyc_df.animalname == "ZORRO") & (all_dogs_nyc_df.breedname == "Yorkshire Terrier")][["zipcode","breedname","animalname","animalgender","animalbirth","licenseissueddate","licenseexpireddate","extract_year","dupe","true_dupe"]])

#print(all_dogs_repeated_order[all_dogs_repeated_order.animalname == "ZSOZSO"])

# I'm not sure why this has *more* rows than doing the same thing without groupby...
#print(all_dogs_nyc_df[(all_dogs_nyc_df.true_dupe == "dupe") & (all_dogs_nyc_df.animalbirth >= 2020)].reset_index(drop=True).groupby(["zipcode","breedname","animalname","animalgender","animalbirth"]).size().sort_values(ascending=False))

#print(all_dogs_nyc_df[(all_dogs_nyc_df.dupe == True) & (all_dogs_nyc_df.true_dupe == "")][["zipcode","animalname","breedname","animalgender","animalbirth","licenseissueddate","licenseexpireddate","extract_year"]])

#print(all_dogs_nyc_df[all_dogs_nyc_df["licenseexpireyear"] == all_dogs_nyc_df["licenseissueyear"]][["zipcode","breedname","animalname","animalgender","animalbirth","licenseissueddate","licenseexpireddate","extract_year"]])

#print(all_dogs_nyc_df[all_dogs_nyc_df.animalname == "ZULU"][["zipcode","breedname","animalname","animalgender","animalbirth","licenseissueddate","licenseexpireddate","extract_year"]])

# print("Number of entries in dataset:",len(all_dogs_nyc_df))
# print("Number of (seemingly) duplicate entries in dataset:",len(all_dogs_nyc_df[all_dogs_nyc_df["true_dupe"] == "dupe"]))
# print("Remaining dogs:", len(all_dogs_nyc_df) - len(all_dogs_nyc_df[all_dogs_nyc_df["true_dupe"] == "dupe"]))

#print(all_dogs_nyc_df[(all_dogs_nyc_df.animalname == "ZOEY") & (all_dogs_nyc_df.animalbirth == 2013)][["zipcode","breedname","animalgender","animalbirth","licenseissueddate","licenseexpireddate","extract_year","dupe","true_dupe"]].tail(25))

all_dogs_nyc_clean = all_dogs_nyc_df[all_dogs_nyc_df.true_dupe != "dupe"][orig_columns].reset_index(drop=True)
### If all_dogs_nyc_clean ever throws a SettingWithCopyWarning, I may need to add .copy().

