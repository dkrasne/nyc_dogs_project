# NYC Open Data Dog Licensing Dataset
# https://data.cityofnewyork.us/resource/nu7n-tubp.json
# app token: I2gtjPD9w788AnMk32W8lmmL8

# ACS 5-Year 2017-2021
# https://www.census.gov/data/developers/data-sets/acs-5year.html
# API key: b08c2ad933f74f40428c15d03a91c23b7e53854d


from sodapy import Socrata
import pandas as pd
import requests
import json
import csv
import geopandas as gpd
import matplotlib

###################### LOADING NYC DOG LICENSING DATA ########################
nyc_data_url = 'data.cityofnewyork.us'
nyc_data_set = "nu7n-tubp"
nyc_app_token = "I2gtjPD9w788AnMk32W8lmmL8" #don't share this
client = Socrata(nyc_data_url,nyc_app_token)

client.timeout = 60

###################### NYC DATASET METADATA ###################

metadata = client.get_metadata(nyc_data_set)
# for item in metadata:
#     print(item)
#     print(metadata[item])

for item in metadata["columns"]:
    if item["name"] == "BreedName" or item["name"] == "ZipCode" or item["name"] == "AnimalName":
        pass # comment out this line and uncomment the following two if this is needed
        # print(item['name'])
        # print(item['cachedContents']['top'])

####################### NECESSARY SETUP #########################

all_dogs_dict = client.get(nyc_data_set, limit=10000) # do client.get_all(data_set) for all records
all_dogs_df = pd.DataFrame.from_records(all_dogs_dict)
#print(all_dogs_df.columns)
#print(all_dogs_df.info())

# Some dogs are registered with invalid or outside-of-NYC zipcodes, so this helps to get a list of only those definitively placeable in NYC.
manhattan_zips = [str(zipcode) for zipcode in list(range(10001,10283))]
staten_zips = [str(zipcode) for zipcode in list(range(10301,10315))]
bronx_zips = [str(zipcode) for zipcode in list(range(10451,10476))]
queens_zips = [str(zipcode) for zipcode in list(range(11004,11110))] + [str(zipcode) for zipcode in list(range(11351,11698))]
brooklyn_zips = [str(zipcode) for zipcode in list(range(11201,11257))]
nyc_zips = manhattan_zips + staten_zips + bronx_zips + queens_zips + brooklyn_zips

#################### LOADING NYC ZCTA SHAPEFILES ######################

shape_url = "https://data.cityofnewyork.us/api/geospatial/pri4-ifjk?date=20231107&accessType=DOWNLOAD&method=export&format=Shapefile"
zcta_shapes = gpd.read_file(shape_url)
nyc_zcta_shapes = zcta_shapes[["modzcta","pop_est","geometry"]].rename(columns={"modzcta": "zipcode"})

###################### LOADING ACS 5-YR 2017-2021 DATA #######################
### moved this to separate program so it didn't do an API call every time.
# census_api_key = "b08c2ad933f74f40428c15d03a91c23b7e53854d" #don't share this
# acs_median_income_variable = "B19013_001E"
# acs_base_url = "https://api.census.gov/data/2021/acs/acs5"
# acs_zipcode_income = []
# i = 0
# for zipcode in nyc_zips:
#     acs_url = "{acs_base_url}?get=NAME,{acs_variable}&for=zip%20code%20tabulation%20area:{zipcode}&key={census_api_key}"\
#     .format(acs_base_url = acs_base_url,acs_variable = acs_median_income_variable,census_api_key=census_api_key,\
#             zipcode=zipcode)
#     acs_results = requests.get(acs_url)
#     try:
#         acs_json = acs_results.json()
#     except requests.exceptions.JSONDecodeError:
#         pass
#     acs_dict = {key:value for key, value in zip(acs_json[0],acs_json[1])}
#     acs_dict.pop("NAME")
#     acs_zipcode_income.append(acs_dict)
# for item in acs_zipcode_income:
#     item["zipcode"] = item.pop("zip code tabulation area")
#     item["median_income"] = int(item.pop("B19013_001E"))

# with open("acs_zipcode_income.csv","w",newline="") as income_csv:
#     csv_writer = csv.writer(income_csv)
#     csv_writer.writerow(["zipcode","median_income"])
#     for dict in acs_zipcode_income:
#         csv_writer.writerow(dict.values())

############# DOING STUFF NOT IN PANDAS #######################

breed_list = []

for dog in all_dogs_dict:
    if dog["breedname"] not in breed_list:
        breed_list.append(dog["breedname"])

name_list = []
for dog in all_dogs_dict:
    if dog["animalname"] not in name_list:
        name_list.append(dog["animalname"])

name_list.sort()
breed_list.sort()

breed_dict = {}
name_dict = {}
zip_breed_dict = {}

for dog in all_dogs_dict:
    name = dog["animalname"]
    breed = dog["breedname"]
    zipcode = dog["zipcode"]
    breed_dict.setdefault(breed, 0)
    breed_dict[breed] += 1
    name_dict.setdefault(name, 0)
    name_dict[name] += 1
    zip_breed_dict.setdefault(zipcode,{})
    zip_breed_dict[zipcode].setdefault(breed, 0)
    zip_breed_dict[zipcode][breed] += 1

### These worked and then stopped working; maybe a dict() conflict with an imported package?
#breed_dict_sorted = dict(sorted(breed_dict.items(), key=lambda x:x[1]))
#name_dict_sorted = dict(sorted(name_dict.items(), key=lambda x:x[1]))

# print(len(breed_dict_sorted))
# print(len(name_dict_sorted))
# print(len(name_list))

zip_list = []
for dog in all_dogs_dict:
    if dog["zipcode"] in nyc_zips:
        if dog["zipcode"] not in zip_list:
            zip_list.append(dog["zipcode"])
zip_list.sort()

# for zipcode in zip_list:
#     print(zipcode,zip_breed_dict.get(zipcode))

############### DOING STUFF IN PANDAS ###############

nyc_zips_income = pd.read_csv("C:/workspace/nyc_dogs_project/acs_zipcode_income.csv", dtype={
    "zipcode": "string",
    "median_income": "int64"
})
#nyc_zips_income = nyc_zips_income.astype({'zipcode': 'string'})

all_dogs_df = all_dogs_df[all_dogs_df["zipcode"].isin(nyc_zips)].reset_index(drop=True)
#print(all_dogs_df["animalname"][4])

all_dogs_df.drop_duplicates(subset=["animalname","animalgender","animalbirth","breedname","zipcode"],inplace=True,ignore_index=True)
#print(all_dogs_df.head())

#print(all_dogs_df.info(),nyc_zips_income.info())

all_dogs_zip_income = pd.merge(all_dogs_df,nyc_zips_income,how="left")

breeds_by_zip = all_dogs_zip_income.groupby(["zipcode","median_income","breedname"]).animalbirth.count().reset_index().rename(columns={"animalbirth": "count"})
#print(breeds_by_zip.info())
breeds_by_zip = breeds_by_zip[breeds_by_zip.breedname != "Unknown"].reset_index(drop=True)
breeds_by_zip = breeds_by_zip[breeds_by_zip.breedname != "Not Provided"].reset_index(drop=True)
#print(breeds_by_zip[(breeds_by_zip.count > 1) & (breeds_by_zip.breedname != "Unknown")].breedname.unique())
#print(breeds_by_zip.info())

breeds_by_zip["max_count"] = breeds_by_zip.groupby("zipcode")["count"].transform("max")
#print(breeds_by_zip.info())
#print(breeds_by_zip[breeds_by_zip["count"] == breeds_by_zip.max_count]) # most populous breed(s) per zipcode
#print(breeds_by_zip.groupby("zipcode").breedname.nunique()) # number of breeds per zipcode

nyc_zips_income_geoms = nyc_zcta_shapes.merge(nyc_zips_income, how="inner")
#nyc_zips_income_geoms.plot(column="median_income")
# 

## This still has correct delineation in plot:
breeds_by_zip_geoms = nyc_zcta_shapes.merge(breeds_by_zip,how="outer")
#print(type(breeds_by_zip_geoms),breeds_by_zip_geoms.info())

## This does NOT have the correct delineation in plot:
breeds_by_zip_geoms.plot(column="median_income")
#breeds_gdf = gpd.GeoDataFrame(breeds_by_zip_geoms)
#breeds_gdf.plot()