import requests
import json
import csv
import time

manhattan_zips = [str(zipcode) for zipcode in list(range(10001,10283))]
staten_zips = [str(zipcode) for zipcode in list(range(10301,10315))]
bronx_zips = [str(zipcode) for zipcode in list(range(10451,10476))]
queens_zips = [str(zipcode) for zipcode in list(range(11004,11110))] + [str(zipcode) for zipcode in list(range(11351,11698))]
brooklyn_zips = [str(zipcode) for zipcode in list(range(11201,11257))]
nyc_zips = manhattan_zips + staten_zips + bronx_zips + queens_zips + brooklyn_zips

###################### LOADING ACS 5-YR 2017-2021 DATA #######################
census_api_key = "b08c2ad933f74f40428c15d03a91c23b7e53854d" #don't share this
acs_median_income_variable = "B19013_001E"
acs_base_url = "https://api.census.gov/data/2021/acs/acs5"
acs_zipcode_income = []
i = 0
for zipcode in nyc_zips:
    acs_url = "{acs_base_url}?get=NAME,{acs_variable}&for=zip%20code%20tabulation%20area:{zipcode}&key={census_api_key}"\
    .format(acs_base_url = acs_base_url,acs_variable = acs_median_income_variable,census_api_key=census_api_key,\
            zipcode=zipcode)
    acs_results = requests.get(acs_url)
    try:
        acs_json = acs_results.json()
    except requests.exceptions.JSONDecodeError:
        pass
    acs_dict = {key:value for key, value in zip(acs_json[0],acs_json[1])}
    acs_dict.pop("NAME")
    acs_zipcode_income.append(acs_dict)
    time.sleep(.125)
for item in acs_zipcode_income:
    item["zipcode"] = item.pop("zip code tabulation area")
    item["median_income"] = int(item.pop("B19013_001E"))

with open("acs_zipcode_income.csv","w",newline="") as income_csv:
    csv_writer = csv.writer(income_csv)
    csv_writer.writerow(["zipcode","median_income"])
    for dict in acs_zipcode_income:
        csv_writer.writerow(dict.values())
