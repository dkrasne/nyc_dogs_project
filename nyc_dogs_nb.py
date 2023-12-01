#!/usr/bin/env python
# coding: utf-8

# # Dogs of New York

# In[1]:


from sodapy import Socrata  # for getting NYC Open Data
import pandas as pd
import requests
import json
import csv
import geopandas as gpd
from matplotlib import pyplot as plt
import datetime
import numpy as np
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
import re


# Download data from NYC Open Data and make into Pandas dataframe.

# In[3]:


## Moved to a separate file so it didn't download every time.

# nyc_data_url = 'data.cityofnewyork.us'
# nyc_data_set = "nu7n-tubp"
# nyc_app_token = "I2gtjPD9w788AnMk32W8lmmL8" #don't share this
# client = Socrata(nyc_data_url,nyc_app_token)

# client.timeout = 60

# all_dogs_dict = client.get_all(nyc_data_set)
# all_dogs_df = pd.DataFrame.from_records(all_dogs_dict)

# all_dogs_df.to_csv("all_nyc_dogs.csv",index=False) # use as backup


# In[4]:


all_dogs_df = pd.read_csv("C:/workspace/nyc_dogs_project/all_nyc_dogs.csv", dtype={"zipcode": str}, parse_dates=["licenseissueddate","licenseexpireddate"])


# Some dogs are registered with invalid or outside-of-NYC zipcodes, so we need to get a list of only those definitively placeable in NYC. For that, we need a list of NYC zipcodes.

# In[5]:


manhattan_zips = [str(zipcode) for zipcode in range(10001,10283)]
staten_zips = [str(zipcode) for zipcode in range(10301,10315)]
bronx_zips = [str(zipcode) for zipcode in range(10451,10476)]
queens_zips = [str(zipcode) for zipcode in range(11004,11110)] + [str(zipcode) for zipcode in range(11351,11698)]
brooklyn_zips = [str(zipcode) for zipcode in range(11201,11257)]
nyc_zips = manhattan_zips + staten_zips + bronx_zips + queens_zips + brooklyn_zips


# In[6]:


all_dogs_nyc_df = all_dogs_df[all_dogs_df["zipcode"].isin(nyc_zips)].reset_index(drop=True)

all_dogs_nyc_df.animalname = all_dogs_nyc_df.animalname.fillna("NOT PROVIDED")
all_dogs_nyc_df.animalgender = all_dogs_nyc_df.animalgender.fillna("-")
# all_dogs_nyc_df["licenseissueddate"] = pd.to_datetime(all_dogs_nyc_df["licenseissueddate"])
# all_dogs_nyc_df["licenseexpireddate"] = pd.to_datetime(all_dogs_nyc_df["licenseexpireddate"])

orig_columns = ["animalname","animalgender","animalbirth","breedname","zipcode","licenseissueddate","licenseexpireddate","extract_year"]


# ## Cleaning the dog licensing dataset

# In[7]:


all_dogs_nyc_df["breedname"] = all_dogs_nyc_df.breedname.apply(lambda x: x.title())

def clean_breed(breed):
    breed = breed.strip()
    breed = re.sub(r".*Breed.*","Unknown", breed)
    breed = re.sub(r".*Mutt.*","Unknown", breed)
    breed = re.sub(r"Unknown.*", "Unknown", breed)
    breed = re.sub(r"^Mix$", "Unknown", breed)
    breed = re.sub(r"^Hound$|^Hound Mix$", "Unknown", breed)
    breed = re.sub(r"^Hound ?\W? ?(.+)", r"\1 Crossbreed", breed)
    breed = re.sub(r"Possibly.+", "Unknown", breed)
    breed = re.sub("Canine", "Unknown", breed)
    breed = re.sub("Rabies,Bordetella", "Unknown", breed)
    breed = re.sub(r"\.", " ", breed)
    breed = re.sub(r"\s\s+", " ", breed)
    breed = re.sub(r"\d+", "Not Provided", breed)
    breed = re.sub("Pitt", "Pit", breed)
    breed = re.sub(r"^Pit$", "Pit Bull", breed)
    breed = re.sub(r".Doodle", "doodle", breed)
    breed = re.sub("Havenese", "Havanese", breed)
    breed = re.sub(r"(\S)(\/)(\S)", r"\1 \2 \3", breed)
    breed = re.sub(r"(\/)(\S)", r"\1 \2", breed)
    breed = re.sub(r"(\S)(\/)", r"\1 \2", breed)
    breed = re.sub(r"Mixed|\bX\b", "Crossbreed", breed)
    breed = re.sub("Mix", "Crossbreed", breed)
    breed = re.sub(r"Aussie\b", "Australian Shepherd", breed)
    breed = re.sub(r"\bMini\b", "Miniature", breed)
    breed = re.sub(r"(^Miniature) (.+)( \/)", r"\2, Miniature\3", breed)
    breed = re.sub(r"(^Miniature) (.+)", r"\2, Miniature", breed)
    breed = re.sub(r"([a-z]\b)( Miniature$)", r"\1,\2", breed)
    breed = re.sub(r"Rotti[a-z]\b", "Rottweiler", breed)
    breed = re.sub(r"(.+) (Welsh Corgi)", r"\2, \1", breed)
    breed = re.sub(r"Russ\b", "Russell", breed)
    breed = re.sub(r"Ter{1,2}i?\b", "Terrier", breed)
    breed = re.sub("Shitzu", "Shih Tzu", breed)
    breed = re.sub(r"(^Toy) (.+)", r"\2, \1", breed)
    breed = re.sub(" Crossbreed, Miniature", ", Miniature, Crossbreed", breed)
    breed = re.sub(r"Sheph?ard", "Shepherd", breed)
    breed = re.sub(r"Shep\b", "Shepherd", breed)
    breed = re.sub(r"Germ?\b", "German", breed)
    breed = re.sub(r"Aust\b", "Australian", breed)
    breed = re.sub(r"Pome?r?\b", "Pomeranian", breed)
    breed = re.sub(r"Retre?iver", "Retriever", breed)
    breed = re.sub("Lab Retriever", "Labrador Retriever", breed)
    breed = re.sub(r"Lab\b", "Labrador Retriever", breed)
    breed = re.sub(r"Mostly (.+)", r"\1 Crossbreed", breed)
    breed = re.sub(r"Pin\b", "Pinscher", breed)
    breed = re.sub("Pincher", "Pinscher", breed)
    breed = re.sub(r"Spa?n?\b", "Spaniel", breed)
    breed = re.sub(r"(Cav\b)(.+) Crossbreed", "Cavalier King Charles Spaniel Crossbreed", breed)
    breed = re.sub(r"(Cav\b)(.+)", "Cavalier King Charles Spaniel", breed)
    breed = re.sub(r"Malt\b", "Maltese", breed)
    breed = re.sub(r"Maltes\b", "Maltese", breed)
    breed = re.sub(r"Malt.{0,8}[Pp]om.+|Pom.{0,10}Malt.+", "Maltipom", breed)
    breed = re.sub(r"Coo?c..*[Pp]ood?l?e?|Poo.+Spaniel", "Cockapoo", breed)
    breed = re.sub(r"Pe.?k.*[pP]ood?l?e?", "Peekapoo", breed)
    breed = re.sub(r"Ame?r?\b", "American", breed)
    breed = re.sub(r"America\b", "American", breed)
    breed = re.sub(r"Staffo?r?d?\b", "Staffordshire", breed)
    breed = re.sub(r"Ausi?e?\b", "Australian", breed)
    breed = re.sub("Hungarian Puli", "Puli", breed)
    breed = re.sub("German Shepherd Dog", "German Shepherd", breed)
    breed = re.sub(r".*\/ \?", "Unknown", breed)
    breed = re.sub("Yorkie Terrier", "Yorkshire Terrier", breed)
    breed = re.sub(r"Yorkie?", "Yorkshire Terrier", breed)
    breed = re.sub("Terriershire", "Terrier", breed)
    breed = re.sub("Bassett", "Basset", breed)
    breed = re.sub("Doxen", "Dachshund", breed)
    breed = re.sub(r"Doxie?", "Dachshund", breed)
    breed = re.sub(r"Da?[sc]{1,2}hs?h?a?u?n?\b", "Dachshund", breed)
    breed = re.sub(r"Brussel\b", "Brussels", breed)
    breed = re.sub(r".*Morky?i?e?.*", "Morkie", breed)
    breed = re.sub(r"\b- ", " / ", breed)
    breed = re.sub(" - ", " / ", breed)
    breed = re.sub(r"^Cur$", "Unknown", breed)
    breed = re.sub(r"\?", "", breed)
    breed = re.sub(r"\/ Crossbreed$", "Crossbreed", breed)
    breed = re.sub(r"Mis\b", "Crossbreed", breed)
    breed = re.sub(r"Peke\b.+P.[^m].*", "Pekingese / Poodle Crossbreed", breed)
    breed = re.sub(r"Cock.?[aA].?[pP]oo", "Cockapoo", breed)
    breed = re.sub(r"Russel\b", "Russell", breed)
    breed = re.sub("Pharoh", "Pharaoh", breed)
    breed = re.sub("Prynese", "Pyrenees", breed)
    breed = re.sub(r"(.*) Coonhound$", r"Coonhound, \1", breed)
    breed = re.sub(r".+ Or .+", "Unknown", breed)
    breed = re.sub(r"Cav.?[aA].?[Pp]oo", "Cavapoo", breed)
    breed = re.sub("Pom-Chi", "Pomchi", breed)
    breed = re.sub(r"Chih?u?a?\b", "Chihuahua", breed)
    breed = re.sub(r"He[^e]ler", "Heeler", breed)
    breed = re.sub("Pitbull", "Pit Bull", breed)
    breed = re.sub("Pit Bull Terrier", "Pit Bull", breed)
    breed = re.sub(r"Bl N.?s.?", "Blue Nose", breed)
    breed = re.sub(r"([a-z])([A-Z])", r"\1 \2", breed)
    breed = re.sub("American Pit Bull", "Pit Bull, American", breed)
    breed = re.sub("Red Nose Pit Bull", "Pit Bull, Red Nose", breed)
    breed = re.sub("Blue Nose Pit Bull", "Pit Bull, Blue Nose", breed)
    breed = re.sub("Brindle Pit Bull", "Pit Bull, Brindle", breed)
    breed = re.sub("Tsu", "Tzu", breed)
    breed = re.sub(r"Shi.{0,4}[tT].?[Zzs][uo]?\b", "Shih Tzu", breed)
    breed = re.sub(r"Shi.?.?[pP]oo", "Shih-Poo", breed)
    breed = re.sub("Shih Tuz & Poodle Crossbreed", "Shih-Poo", breed)
    breed = re.sub("Shih Tzu / Poodle", "Shih-Poo", breed)
    breed = re.sub(r"Maltese \/? ?Shih Tzu|Shih Tzu \/ Maltese|Shi.{0,8}tese|Shi.{0,3}[Mm]alt", "Malshi", breed)
    breed = re.sub(r"([^dk]) And", r"\1 \/", breed)
    breed = re.sub("Poddle&Maltese Crossbreed", "Maltipoo", breed)
    breed = re.sub(r"Malt.{0,6}[pP]ood?l?e? ?C?r?o?s?s?b?r?e?e?d?", "Maltipoo", breed)
    breed = re.sub(r"Yorkshire.{1,12}[Pp]ood?l?e? ?C?r?o?s?s?b?r?e?e?d?", "Yorkipoo", breed)
    breed = re.sub(r"Bea\b", "Beagle", breed)
    breed = re.sub(r"Eng\b", "English", breed)
    breed = re.sub(r"Corg\b", "Corgi", breed)
    breed = re.sub(r"Ch\b", "Charles", breed)
    breed = re.sub(r"Bull Dog\b", "Bulldog", breed)
    breed = re.sub("Chinese Shar-Pei", "Shar-Pei, Chinese", breed)
    breed = re.sub("Sharpei", "Shar-Pei", breed)
    breed = re.sub(r"Jap\b", "Japanese", breed)
    breed = re.sub(r"^Crossbreed \/ ?(.+)", r"\1 Crossbreed", breed)
    breed = re.sub(r"(Jack Russell )&.+", r"\1Crossbreed", breed)
    breed = re.sub(r"L[^h]?asa", "Lhasa", breed)
    breed = re.sub(r".*Jindo.*", "Korean Jindo", breed)
    breed = re.sub("Blackmouthcur", "Black Mouth Cur", breed)
    breed = re.sub(r".*Dogue De Bordeaux.*", "Dogue De Bordeaux", breed)
    breed = re.sub("S C", "Soft Coated", breed)
    breed = re.sub(r"Soft.+W.+ ", "Soft Coated Wheaten", breed)
    breed = re.sub(r"^Wh?eat.n", "Soft Coated Wheaten", breed)
    breed = re.sub("Cttle Dg", "Cattle Dog", breed)
    breed = re.sub("Cattledog", "Cattle Dog", breed)
    breed = re.sub("Collie, Border", "Border Collie", breed)
    breed = re.sub(r"St\b", "Saint", breed)
    breed = re.sub(r"Gold\b", "Golden", breed)
    breed = re.sub(r"Retr?\b", "Retriever", breed)
    breed = re.sub(r"\+", "/", breed)
    breed = re.sub(r"Fr\b", "French", breed)
    breed = re.sub("French Bulldog", "Bulldog, French", breed)
    breed = re.sub("American Bulldog", "Bulldog, American", breed)
    breed = re.sub("Boston Bulldog", "Bulldog, Boston", breed)
    breed = re.sub(r"Olde English Bulldog\b", "Olde English Bulldogge", breed)
    breed = re.sub(r"\\ English Bulldog", "\ Bulldog, English", breed)
    breed = re.sub(" Crossbreed ", " / ", breed)
    breed = re.sub(r"\\", "", breed)
    breed = re.sub("Crossbreed Crossbreed", "Crossbreed", breed) # keep this near end
    #breed = re.sub(r"(.*) ?(Pit Bull)( ?.*)", r"\2, \1\3", breed)
    breed = re.sub(r"\/.+Crossbreed", "Crossbreed", breed) # keep second to last, probably
    breed = re.sub(r"(.+) \/ \1", r"\1", breed) # keep this last

    return breed

all_dogs_nyc_df["breedname"] = all_dogs_nyc_df.breedname.apply(clean_breed)


# In[8]:


breed_list = all_dogs_nyc_df.breedname.unique()
breed_list
with open("breedlist.csv", "w", newline="\n", encoding="utf-8") as output_file:
    breedlist = csv.writer(output_file)
    breedlist.writerow(["breed"])
    for breed in breed_list:
        breedlist.writerow([breed])

print(len(breed_list))


# In[9]:


# breed_list = [breed.title() for breed in breed_list]
# print(len(breed_list))

# def unique_list(list):
#     new_list = []
#     for item in list:
#         if item in new_list:
#             pass
#         else:
#             new_list.append(item)
#     return new_list

# breed_list = unique_list(breed_list)
# print(len(breed_list))


# ### Dealing with duplicates

# Dogs recur in the dataset every time their license is renewed (whether due to expiration or replacement). Thus we need to deal with entries that are probable duplicates: dogs that share a name, gender, birth year, and breed.

# In[10]:


print("Number of entries in dataset:",len(all_dogs_nyc_df))
print("Number of (seemingly) duplicate entries in dataset:",len(all_dogs_nyc_df)-len(all_dogs_nyc_df.drop_duplicates(subset=["animalname","animalgender","animalbirth","breedname","zipcode"])))


# Overwrite df with df grouped by all columns, by size (which turns out to be max 3 in any group; this is reasonable for duplicated extraction); drop size column. While the result is the same as simply dropping duplicated entries using `.drop_duplicates()`, it allows us to see how many unquestionable duplicates exist for any given dog.

# In[11]:


all_dogs_nyc_df = all_dogs_nyc_df.groupby(orig_columns, dropna=False).size().reset_index().rename(columns={0: "size"})
print(all_dogs_nyc_df.head(10))
all_dogs_nyc_df.drop(columns="size",inplace=True)


# In[12]:


all_dogs_nyc_df["dupe"] = all_dogs_nyc_df.duplicated(subset=["animalname","animalgender","animalbirth","breedname","zipcode"])
all_dogs_nyc_df = all_dogs_nyc_df.reset_index().rename(columns={"index": "old_index"})


# However, since nearly 25000 of those have an unknown breed, they may well *not* all be duplicates. Including these possible duplicates would affect the total number of dogs, but not the total number of any given breed. Additionally, it is unlikely that the numerous "duplicate" French Bulldogs with an unknown name that were born in 2020 and live in zipcode 10011 are all the same dog. If we assume no more than two re-registrations per year of life for any given dog (already a generous estimation), we can retain more dogs in the dataset.

# In[13]:


print(all_dogs_nyc_df[all_dogs_nyc_df["dupe"] == True].groupby("breedname").size().sort_values(ascending=False))
print(all_dogs_nyc_df[all_dogs_nyc_df["dupe"] == True].groupby(["zipcode","breedname","animalname","animalgender","animalbirth"]).size().sort_values(ascending=False))


# #### Using license issue and expiry dates to help determine duplicates

# Dog licenses can be renewed for up to five years. Some license expiry years are therefore improbable, given that they are more than five years past even the final extraction date. However, as there is nothing to suggest that the dog itself is problematic, and we are not looking at registration expirations except to help identify duplicates, we can leave these in the dataset.

# In[14]:


all_dogs_nyc_df["licenseissueyear"] = all_dogs_nyc_df["licenseissueddate"].dt.year
all_dogs_nyc_df["licenseexpireyear"] = all_dogs_nyc_df["licenseexpireddate"].dt.year
all_dogs_nyc_df.groupby("licenseexpireyear").size()


# In[15]:


all_dogs_nyc_df["lxmonth"] = all_dogs_nyc_df.licenseexpireddate.dt.month
all_dogs_nyc_df["lxday"] = all_dogs_nyc_df.licenseexpireddate.dt.day


# We can consider a dog duplicated if they share `["animalname","animalgender","animalbirth","breedname","zipcode"]` and the **year** of the earlier license expiry is within one year (above or below) of the later license issue. Furthermore, since a renewed license often appears to expire on the same day and month as the previous one, we can also use that for identifying additional duplicates.

# In[16]:


repeat_index_list = []
for rownum in range(len(all_dogs_nyc_df["old_index"])):
    #current_row = all_dogs_nyc_df.dupe.iloc[rownum]
    if (all_dogs_nyc_df.dupe.iloc[rownum] == True) and ((all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]) or (all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]+1)or (all_dogs_nyc_df.licenseissueyear.iloc[rownum] == all_dogs_nyc_df.licenseexpireyear.iloc[rownum-1]-1)):
        repeat_index_list.append(rownum)
    elif(all_dogs_nyc_df.dupe.iloc[rownum] == True) and (((all_dogs_nyc_df.lxmonth.iloc[rownum] == all_dogs_nyc_df.lxmonth.iloc[rownum-1]) and (all_dogs_nyc_df.lxday.iloc[rownum] == all_dogs_nyc_df.lxday.iloc[rownum-1])) or ((all_dogs_nyc_df.lxmonth.iloc[rownum] == all_dogs_nyc_df.lxmonth.iloc[rownum-2]) and (all_dogs_nyc_df.lxday.iloc[rownum] == all_dogs_nyc_df.lxday.iloc[rownum-2]))):
        repeat_index_list.append(rownum)
    else:
        repeat_index_list.append(None)


# In[17]:


## This took SO much longer and then failed.

# repeat_index_list = []
# for rownum in range(len(all_dogs_nyc_df["old_index"])):
#     current_dog = all_dogs_nyc_df.iloc[rownum]
#     prev_dog = all_dogs_nyc_df.iloc[rownum-1]
#     prev2_dog = all_dogs_nyc_df.iloc[rownum-2]
#     next_dog = all_dogs_nyc_df.iloc[rownum+1] #this causes problems eventually.
#     if (current_dog.dupe == True) and ((current_dog.licenseissueyear == prev_dog.licenseexpireyear) or (current_dog.licenseissueyear == prev_dog.licenseexpireyear+1) or (current_dog.licenseissueyear == prev_dog.licenseexpireyear-1)):
#         repeat_index_list.append(rownum)
#     elif(current_dog.dupe == True) and (((current_dog.lxmonth == prev_dog.lxmonth) and (current_dog.lxday == prev_dog.lxday)) or ((current_dog.lxmonth == prev2_dog.lxmonth) and (current_dog.lxday == prev2_dog.lxday))):
#         repeat_index_list.append(rownum)
#     else:
#         repeat_index_list.append(None)


# In[18]:


all_dogs_nyc_df_copy = all_dogs_nyc_df.copy().reset_index()
all_dogs_nyc_df_copy.iloc[[5]].index
print(len(all_dogs_nyc_df_copy))
print(all_dogs_nyc_df_copy.old_index.nunique())
# using all_dogs_nyc_df_copy, try to define a check_dupe() function that I can apply instead of doing the above loop.
# This may have something helpful: https://stackoverflow.com/questions/13331698/how-to-apply-a-function-to-two-columns-of-pandas-dataframe


# In[19]:


true_dupe = pd.Series(repeat_index_list)
all_dogs_nyc_df["true_dupe"] = true_dupe.apply(lambda x: "" if pd.isnull(x) else "dupe")
all_dogs_nyc_clean = all_dogs_nyc_df[all_dogs_nyc_df.true_dupe != "dupe"][orig_columns].reset_index(drop=True)


# **Need to clean breed names, now. E.g., "mini" vs "miniature," or what comes after/before a comma.**

# ## Analyzing the Cleaned Dataset

# In[20]:


all_dogs_num = len(all_dogs_nyc_clean)
all_dogs_num


# In[21]:


all_dogs_nyc_by_breed = all_dogs_nyc_clean.groupby("breedname").size().reset_index().rename(columns={0: "num_breed"})
all_dogs_nyc_by_breed["breed_pct"] = all_dogs_nyc_by_breed.num_breed / all_dogs_num


# (Potentially drop breeds that only occur once; while these are dogs that exist in the city, and thus should be included in the total number, they are not interesting within the dataset. However, they may be needed later.)

# In[22]:


# all_dogs_nyc_by_breed = all_dogs_nyc_by_breed[all_dogs_nyc_by_breed.num_breed > 1]
repeat_breeds = all_dogs_nyc_by_breed[all_dogs_nyc_by_breed.num_breed > 30].breedname
print("Number of repeated breeds:",len(repeat_breeds))
all_dogs_nyc_by_breed.sort_values("breed_pct",ascending=False)


# In[23]:


def zip_to_borough(zipcode):
    if zipcode in manhattan_zips:
        return "Manhattan"
    elif zipcode in bronx_zips:
        return "Bronx"
    elif zipcode in brooklyn_zips:
        return "Brooklyn"
    elif zipcode in queens_zips:
        return "Queens"
    elif zipcode in staten_zips:
        return "Staten Island"
    else:
        return None


# In[24]:


all_dogs_nyc_by_breed_borough = all_dogs_nyc_clean
all_dogs_nyc_by_breed_borough["borough"] = all_dogs_nyc_by_breed_borough.zipcode.apply(zip_to_borough)
all_dogs_manhattan_by_breed = all_dogs_nyc_by_breed_borough[all_dogs_nyc_by_breed_borough.borough == "Manhattan"].groupby("breedname").size().reset_index().rename(columns={0: "num_breed"})
all_dogs_manhattan_by_breed["breed_pct"] = all_dogs_manhattan_by_breed.num_breed / all_dogs_num

all_dogs_bronx_by_breed = all_dogs_nyc_by_breed_borough[all_dogs_nyc_by_breed_borough.borough == "Bronx"].groupby("breedname").size().reset_index().rename(columns={0: "num_breed"})
all_dogs_bronx_by_breed["breed_pct"] = all_dogs_bronx_by_breed.num_breed / all_dogs_num

all_dogs_brooklyn_by_breed = all_dogs_nyc_by_breed_borough[all_dogs_nyc_by_breed_borough.borough == "Brooklyn"].groupby("breedname").size().reset_index().rename(columns={0: "num_breed"})
all_dogs_brooklyn_by_breed["breed_pct"] = all_dogs_brooklyn_by_breed.num_breed / all_dogs_num

all_dogs_queens_by_breed = all_dogs_nyc_by_breed_borough[all_dogs_nyc_by_breed_borough.borough == "Queens"].groupby("breedname").size().reset_index().rename(columns={0: "num_breed"})
all_dogs_queens_by_breed["breed_pct"] = all_dogs_queens_by_breed.num_breed / all_dogs_num

all_dogs_staten_by_breed = all_dogs_nyc_by_breed_borough[all_dogs_nyc_by_breed_borough.borough == "Staten Island"].groupby("breedname").size().reset_index().rename(columns={0: "num_breed"})
all_dogs_staten_by_breed["breed_pct"] = all_dogs_staten_by_breed.num_breed / all_dogs_num


#all_dogs_manhattan_by_breed.head()


# In[25]:


all_dogs_nyc_by_zipcode = all_dogs_nyc_clean.groupby("zipcode").size().reset_index().rename(columns={0: "num_dogs"})
all_dogs_nyc_by_zipcode["dogs_pct"] = all_dogs_nyc_by_zipcode["num_dogs"] / all_dogs_num
all_dogs_nyc_by_zipcode["borough"] = all_dogs_nyc_by_zipcode.zipcode.apply(zip_to_borough)
dogs_pct_max = all_dogs_nyc_by_zipcode.max()["dogs_pct"] # The percentage of NYC's dogs in the zip code with the most dogs
print(dogs_pct_max)
most_dogs = all_dogs_nyc_by_zipcode.max()["num_dogs"] # The highest number of dogs in a single zip code
print(most_dogs)
all_dogs_nyc_by_zipcode.sort_values(by="dogs_pct",ascending=False)


# In[26]:


all_dogs_breed_by_zipcode = all_dogs_nyc_clean.groupby(["zipcode","breedname"]).size().reset_index().rename(columns={0: "num_breed"})
all_dogs_breed_by_zipcode["total_dogs"] = all_dogs_breed_by_zipcode.merge(all_dogs_nyc_by_zipcode,how="left")["num_dogs"]
all_dogs_breed_by_zipcode["breed_pct"] = all_dogs_breed_by_zipcode.num_breed / all_dogs_breed_by_zipcode.total_dogs
#print(all_dogs_breed_by_zipcode.merge(all_dogs_nyc_by_breed,how="left", on="breedname").sort_values(by=["total_dogs","num_breed_x"], ascending=[False,False]))

all_dogs_breed_by_zipcode = all_dogs_breed_by_zipcode.merge(all_dogs_nyc_by_breed,how="left", on="breedname").rename(columns={"num_breed_x": "num_breed_zipcode",
                                                                                                                              "num_breed_y": "num_breed_nyc",
                                                                                                                              "breed_pct_x": "breed_pct_zipcode",
                                                                                                                              "breed_pct_y": "breed_pct_nyc",
                                                                                                                              "total_dogs": "total_dogs_zipcode"})
all_dogs_breed_by_zipcode["breed_pct_diff"] = all_dogs_breed_by_zipcode.breed_pct_zipcode - all_dogs_breed_by_zipcode.breed_pct_nyc
all_dogs_breed_by_zipcode["borough"] = all_dogs_breed_by_zipcode.zipcode.apply(zip_to_borough)
all_dogs_breed_by_zipcode.sort_values(by=["total_dogs_zipcode","num_breed_zipcode"], ascending=[False,False])

##Neither of these worked correctly, whether doing a left merge or a right merge, I'm not sure why.:
#all_dogs_breed_by_zipcode["breed_pct_diff"] = all_dogs_breed_by_zipcode.breed_pct - all_dogs_breed_by_zipcode.merge(all_dogs_nyc_by_breed,how="right", on="breedname")["breed_pct_y"]
#all_dogs_breed_by_zipcode["breed_pct_diff"] = all_dogs_breed_by_zipcode.merge(all_dogs_nyc_by_breed,how="right", on="breedname")["breed_pct_x"] - all_dogs_breed_by_zipcode.merge(all_dogs_nyc_by_breed,how="right", on="breedname")["breed_pct_y"]

## I need to do something like this to account for different numbers of dogs, but neither of these is quite right:
#all_dogs_breed_by_zipcode["breed_pct_resize"] = all_dogs_breed_by_zipcode.breed_pct * all_dogs_breed_by_zipcode.merge(all_dogs_nyc_by_zipcode,how="right", on="zipcode")["dogs_pct"]
#all_dogs_breed_by_zipcode["breed_pct_resize"] = all_dogs_breed_by_zipcode.breed_pct * (all_dogs_breed_by_zipcode.total_dogs/most_dogs)

#all_dogs_breed_by_zipcode[all_dogs_breed_by_zipcode.total_dogs > 5].sort_values(by=["total_dogs","num_breed"],ascending=[False,False])

#all_dogs_breed_by_zipcode.sort_values(by=["total_dogs","num_breed"],ascending=[False,False])


# # NYC Income

# Load ACS 5-year data on median incomes by ZCTA.

# In[27]:


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


# In[28]:


nyc_zips_income = pd.read_csv("C:/workspace/nyc_dogs_project/acs_zipcode_income.csv", dtype={
    "zipcode": "string",
    "median_income": "int64"
})

nyc_median_income = 70663 # number from census.gov: Median household income (in 2021 dollars), 2017-2021


# NEED TO: Join dog dfs with income df, try to do subplot scatterplots using a for loop, e.g., most popular ten breeds in the city relative to median wealth.

# Define a function for getting *`x`* most popular dogs in the city.

# In[29]:


def get_most_popular(num,dataset):
    x_most_popular_dogs = []
    for x in range(0,num):
        try:
            most_popular_dog = dataset[(dataset.breedname != "Unknown") & (dataset.breedname != "Not Provided")].\
                sort_values("breed_pct",ascending=False).reset_index(drop=True).at[x,"breedname"]
        except KeyError:
            most_popular_dog = dataset[(dataset.breedname != "Unknown") & (dataset.breedname != "Not Provided")].\
                sort_values("breed_pct_zipcode",ascending=False).reset_index(drop=True).at[x,"breedname"]
        x_most_popular_dogs.append(most_popular_dog)
    return x_most_popular_dogs

#all_dogs_nyc_by_breed[(all_dogs_nyc_by_breed.breedname != "Unknown") & (all_dogs_nyc_by_breed.breedname != "Not Provided")].\
#    sort_values("breed_pct",ascending=False).reset_index(drop=True)


# In[30]:


all_dogs_breed_by_zipcode_income = all_dogs_breed_by_zipcode.merge(nyc_zips_income, how="left", on="zipcode")
all_dogs_breed_by_zipcode_income.drop_duplicates(inplace=True)
all_dogs_breed_by_zipcode_income = all_dogs_breed_by_zipcode_income[all_dogs_breed_by_zipcode_income.median_income > 0]
all_dogs_breed_by_zipcode_income


# Plot the percentage of most popular identified dog breeds in the city, per zip code, against the median wealth of the zip code, where the zip code has 100 dogs or more.

# In[31]:


x_most_popular_dogs = get_most_popular(15,all_dogs_nyc_by_breed)

get_ipython().run_line_magic('matplotlib', 'inline')
plt.rcParams['figure.figsize'] = (16, 8)
plt.rcParams['figure.dpi'] = 75

colors = {"Manhattan": "blue",
          "Bronx": "green",
          "Queens": "red",
          "Brooklyn": "orange",
          "Staten Island": "black"}

plt.suptitle(f"{len(x_most_popular_dogs)} Most Popular Dogs in NYC\nPopularity vs. Median Income, by Zip Code")
for num in range(1,len(x_most_popular_dogs)+1):
    plt.subplot(3,5,num)
    # plt.scatter(x=all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.breedname == x_most_popular_dogs[num-1]) & (all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100)].median_income, y=all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.breedname == x_most_popular_dogs[num-1]) & (all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100)].breed_pct_zipcode*100, alpha=.15, c=all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.breedname == x_most_popular_dogs[num-1]) & (all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100)].borough.map(colors))
    plt.scatter(x=all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.breedname == x_most_popular_dogs[num-1]) & (all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100)].median_income, y=all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.breedname == x_most_popular_dogs[num-1]) & (all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100)].breed_pct_zipcode*100, alpha=.15)
    plt.ylim(0,.2*100)
    plt.xlabel("Median Income")
    plt.ylabel("% of Dogs in Zip Code")
    plt.title(x_most_popular_dogs[num-1])
plt.subplots_adjust(hspace=.8, wspace=0.6)
plt.show()


# In[32]:


popular_breeds = all_dogs_breed_by_zipcode_income.breedname.isin(x_most_popular_dogs)
dogs_over_100 = all_dogs_breed_by_zipcode_income.total_dogs_zipcode >=100
fig = px.scatter(all_dogs_breed_by_zipcode_income, 
                x=all_dogs_breed_by_zipcode_income[(popular_breeds) & (dogs_over_100)].median_income, 
                y=all_dogs_breed_by_zipcode_income[(popular_breeds) & (dogs_over_100)].breed_pct_zipcode*100, 
                hover_data=[all_dogs_breed_by_zipcode_income[(popular_breeds) & (dogs_over_100)].zipcode], 
                size=all_dogs_breed_by_zipcode_income[(popular_breeds) & (dogs_over_100)].total_dogs_zipcode,
                labels={
                    "x": "Median Income",
                    "y": "% breed in Zip Code",
                    "size": "Total Dogs in Zip Code",
                    "hover_data_0": "Zip Code",
                    "facet_col": "Breed"
                },
                title=f"Popular dogs of New York",
                facet_col=all_dogs_breed_by_zipcode_income[(popular_breeds) & (dogs_over_100)]["breedname"],
                facet_col_wrap=3,
                height=1000,
                width=1000)
fig['data'][0]['showlegend']=True
fig['data'][0]['name']='# dogs in zip code'
fig.show()
    


# In[33]:


def make_traces():
    dogs_over_100 = all_dogs_breed_by_zipcode_income.total_dogs_zipcode >=100
    traces_list = []
    for breed in repeat_breeds:
        current_breed = all_dogs_breed_by_zipcode_income.breedname == breed
        size = all_dogs_breed_by_zipcode_income[(current_breed) & (dogs_over_100)].total_dogs_zipcode
        sizeref = all_dogs_breed_by_zipcode_income.total_dogs_zipcode.max()/(22**2)
        trace_item = go.Scatter(
            #name = breed,
            x = all_dogs_breed_by_zipcode_income[(current_breed) & (dogs_over_100)].median_income, 
            y = all_dogs_breed_by_zipcode_income[(current_breed) & (dogs_over_100)].breed_pct_zipcode, 
            mode = "markers",
            marker=dict(
                size=size,
                sizemode='area',
                sizeref=sizeref,
                sizemin=1
            ),
            customdata = np.stack((all_dogs_breed_by_zipcode_income[(current_breed) & (dogs_over_100)].total_dogs_zipcode), axis=-1),
            hovertemplate='Median Income: %{x}<br>' +
                'Breed in Zip Code: %{y}<br>' +
                'Total Dogs in Zip Code: %{customdata}<br>' +
                'Zip Code: %{text}',
            text = [zipcode for zipcode in all_dogs_breed_by_zipcode_income[(current_breed) & (dogs_over_100)].zipcode],
            visible = False,
            showlegend = True,
            name = 'size indicates<br># dogs in zip code'

            #visible = "legendonly"  # this may actually be the preferable option, but without a dropdown menu; if I do use it, I need to fix the Y axis range.
        )
        traces_list.append(trace_item)
    return traces_list

def make_buttons():
    button_list = []
    for breed in repeat_breeds:
        new_button = dict(label=breed,
                 method="update",
                 args=[{"visible": [True if cur_breed==breed else False for cur_breed in repeat_breeds]}])
        button_list.append(new_button)
    return button_list


all_traces = make_traces()
button_list = make_buttons()


# In[34]:


fig = go.Figure()
for trace in all_traces:
    fig.add_trace(trace)

# fig.update_xaxes(visible=False)
# fig.update_yaxes(visible=False)
#fig.update_yaxes(range=[-.03,all_dogs_breed_by_zipcode_income[(dogs_over_100)].breed_pct_zipcode.max()+.05])
fig.update_yaxes(range=[0,1])

fig.update_layout(
    updatemenus = [dict(
        active = 0,
        buttons = button_list,
        pad_r = 30
    )]
)

fig.update_layout(
    title="Dogs of New York<br><span style='font-size: smaller'>(Select a breed to see its distribution)</span>",
    yaxis_title="% Breed in Zip Code",
    xaxis_title="Median Income of Zip Code<br><span style='font-size: smaller'><i>limited to zip codes with 100 or more dogs</i></span>",
    #yaxis_range=[-.03,all_dogs_breed_by_zipcode_income[(dogs_over_100)].breed_pct_zipcode.max()+.05],
    yaxis_autorange = True,
    yaxis_tickformat = '.2%',
    xaxis_range=[0,all_dogs_breed_by_zipcode_income[(dogs_over_100)].median_income.max()+2000]
)

fig.show()

# maybe a better/simpler approach here: http://webcache.googleusercontent.com/search?q=cache:MJaf3SU3eiwJ:https://python.plainenglish.io/interactive-python-data-visuals-super-slick-plotly-dropdown-menus-6fed21de5d10&client=firefox-b-1-d&sca_esv=583840315&hl=en&gl=us&strip=1&vwsrc=0


# In[35]:


fig = go.Figure()

max_diff = max(all_dogs_breed_by_zipcode_income[dogs_over_100].breed_pct_diff)
min_diff = min(all_dogs_breed_by_zipcode_income[dogs_over_100].breed_pct_diff)
max_range = max(max_diff, abs(min_diff))

fig.update_xaxes(showgrid=True)
fig.update_yaxes(side="right",
                 type="category")

for zipcode in all_dogs_breed_by_zipcode_income[all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100].zipcode.unique():
    dataset = all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.zipcode == zipcode) & (dogs_over_100)].sort_values(by="breed_pct_zipcode",ascending=False).head(20)
    fig.add_trace(
        go.Bar(
            x=dataset.breed_pct_diff,
            y=dataset.breedname,
            orientation="h",
            marker=dict(
                color='rgba(50, 171, 96, 0.6)',
                line=dict(
                    color='rgba(50, 171, 96, 1.0)',
                    width=1),
            ),
            name=zipcode,
            visible=False,
            customdata = np.stack((all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.zipcode == zipcode) & (dogs_over_100)].borough, 
                                   all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.zipcode == zipcode) & (dogs_over_100)].median_income,
                                   all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.zipcode == zipcode) & (dogs_over_100)].total_dogs_zipcode), axis=-1),
            hovertemplate='Dog Breed: %{y}<br>' + 
                'Number of Dogs in Zip Code: %{customdata[2]}<br>' +
                'Difference from City-Wide Percentage of Breed: %{x}<br>' +
                'Borough: %{customdata[0]}<br>' +
                'Median Income: %{customdata[1]}'
        )
    )

steps = []

zipcodes_over_100 = list(all_dogs_breed_by_zipcode_income[all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100].zipcode.unique())
num_zipcodes = all_dogs_breed_by_zipcode_income[all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100].zipcode.nunique()
for zipcode in zipcodes_over_100:
    step = dict(
        method = "update",
        args = [{"visible": [False] * num_zipcodes}],
        #name = zip_to_borough(zipcode),
        label = zipcode
    )
#    visible_list = step["args"][0]["visible"]
    step["args"][0]["visible"][zipcodes_over_100.index(zipcode)] = True  # Toggle i'th trace to "visible"
    steps.append(step)

sliders = [dict(
    active=0,
    currentvalue={"prefix": "<span style='font-size: smaller'><i>Move the slider to see the data!</i></span><br>Zipcode: "},
    pad={"t": 50},
    steps=steps,
    xanchor="left"
)]

fig.update_layout(
    sliders=sliders,
    yaxis_tickmode="linear",
    xaxis_tickformat = '.2%',
#    xaxis_autorange = True,
    xaxis_range = [-(max_range+.05),max_range+.05],
    yaxis_autorange="reversed", 
    title="Most popular dog breeds for selected zipcode,<br>showing difference in proportion relative to citywide proportions",
    margin=dict(r=300)
)

fig.show()


# fig = px.bar(all_dogs_breed_by_zipcode_income[all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100], x=all_dogs_breed_by_zipcode_income[all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100].sort_values(by="breed_pct_zipcode", ascending=False).breed_pct_diff.head(20), y=all_dogs_breed_by_zipcode_income[all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100].sort_values(by="breed_pct_zipcode", ascending=False).breedname.head(20), orientation="h", animation_frame="zipcode")
# fig.update_yaxes(categoryorder="array", 
#                  categoryarray=[all_dogs_breed_by_zipcode_income[(all_dogs_breed_by_zipcode_income.zipcode == zipcode) & 
#                                                                  (all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100)].sort_values(by="breed_pct_zipcode",ascending=False).head(20)
#                                                                   for zipcode in all_dogs_breed_by_zipcode_income[all_dogs_breed_by_zipcode_income.total_dogs_zipcode >= 100].zipcode.unique()])


# In[36]:


manhattan_popular = get_most_popular(10,all_dogs_manhattan_by_breed)
bronx_popular = get_most_popular(10,all_dogs_bronx_by_breed)
brooklyn_popular = get_most_popular(10,all_dogs_brooklyn_by_breed)
queens_popular = get_most_popular(10,all_dogs_queens_by_breed)
staten_popular = get_most_popular(10,all_dogs_staten_by_breed)

for i in range(10):
    print(f"{str(i+1)}. Manhattan: {manhattan_popular[i]}\n\
    Bronx: {bronx_popular[i]}\n\
    Brooklyn: {brooklyn_popular[i]}\n\
    Queens: {queens_popular[i]}\n\
    Staten Island: {staten_popular[i]}")



# In[37]:


# zipcode with highest income
all_dogs_breed_by_zipcode_income.sort_values(by="median_income", ascending=False).head(1)["zipcode"]


# In[38]:


get_ipython().run_line_magic('matplotlib', 'inline')
plt.rcParams['figure.figsize'] = (5, 3)
plt.rcParams['figure.dpi'] = 75
plt.scatter(x=all_dogs_breed_by_zipcode_income.groupby(["zipcode","total_dogs_zipcode","median_income"]).size().reset_index().median_income, y=all_dogs_breed_by_zipcode_income.groupby(["zipcode","total_dogs_zipcode","median_income"]).size().reset_index().total_dogs_zipcode, alpha=.15)
plt.xlabel("Median Income of Zip Code")
plt.ylabel("Number of Dogs in Zip Code")
plt.show()


# ## Mapping the Data

# ACS data is attached to ZCTAs, which aren't entirely analogous to zipcodes due to some zipcodes having no population. NYC created shapefiles of modified ZCTAs for mapping COVID data; these can be used for mapping ACS data and connecting it with the zipcodes of the dog licensing dataset.

# In[39]:


shape_url = "https://data.cityofnewyork.us/api/geospatial/pri4-ifjk?date=20231107&accessType=DOWNLOAD&method=export&format=Shapefile"
zcta_shapes = gpd.read_file(shape_url)
#print(zcta_shapes.info())
nyc_zcta_shapes = zcta_shapes[["modzcta","pop_est","geometry"]].rename(columns={"modzcta": "zipcode"})



# In[40]:


zcta_shapes


# Some zip codes with registered dogs appear not to have a median income. All but two of these are ALSO not included in the zip codes rolled into DOHMH's modified ZCTAs. While they can be included in city-wide dogs, they cannot be included in considerations of individual zip codes.

# In[41]:


all_dogs_breed_by_zipcode_income_2 = all_dogs_breed_by_zipcode.merge(nyc_zips_income, how="left")
zips_no_income = all_dogs_breed_by_zipcode_income_2[all_dogs_breed_by_zipcode_income_2.isna().any(axis=1)].sort_values(by=["total_dogs_zipcode","zipcode"], ascending=[False,True]).zipcode.unique()
#zips_no_income_df = pd.DataFrame({'zipcode': zips_no_income})
#zips_no_income_df[""]
print(zips_no_income)
zips_no_income_zctas = {}
for zipcode in zips_no_income:
    modzcta_row = zcta_shapes[zcta_shapes["zcta"].str.contains(zipcode)]
    if len(modzcta_row) > 0:
        print(modzcta_row)


# for zipcode in all_dogs_breed_by_zipcode.zipcode.unique():
#     zip_income = nyc_zips_income[nyc_zips_income.zipcode == zipcode].drop_duplicates().dropna().reset_index()
#     #print(zip_income)
#     print(zip_income.at[0,"median_income"])


# In[42]:


nyc_zips_income_geoms = nyc_zcta_shapes.merge(nyc_zips_income, how="inner").set_geometry("geometry")
nyc_zips_income_geoms["id"] = nyc_zips_income_geoms.zipcode
print(nyc_zips_income_geoms.head(1))
nyc_zips_income_geoms.plot(column="median_income",legend=True,legend_kwds={"label": "Median Income in USD"}).set_title("Median Income in NYC, by zip code")


# In[43]:


nyc_zips_income_geoms.head(1)


# In[44]:


all_dogs_breed_by_zipcode_income_geoms = nyc_zips_income_geoms.merge(all_dogs_breed_by_zipcode_income, how="left", on=["zipcode","median_income"]).drop_duplicates().reset_index(drop=True)

all_dogs_breed_by_zipcode_income_geoms.set_geometry("geometry").set_index("zipcode")

print(type(all_dogs_breed_by_zipcode_income_geoms))
all_dogs_breed_by_zipcode_income_geoms.head(1)


# In[45]:


# had to convert shapefiles to geojson using an online SHP to GEOJSON converter, since the geometry column of the GeoPandas file wasn't producing useful results.
with open("C:/workspace/nyc_dogs_project/modzcta.geojson") as geojson_file:
    geojson = json.load(geojson_file)

geojson["features"][0]["properties"]
fig = px.choropleth(all_dogs_breed_by_zipcode_income_geoms.query("breedname=='Yorkshire Terrier'"), # using the non-gpd file still worked, but the colors were really pale.(???)
                geojson=geojson,
                locations=all_dogs_breed_by_zipcode_income_geoms.query("breedname=='Yorkshire Terrier'").zipcode,
                color = "breed_pct_diff",
                featureidkey="properties.zcta",
                color_continuous_scale='Picnic',
                color_continuous_midpoint=0)
fig.update_geos(fitbounds="locations", visible=False)
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
fig.show()


# In[49]:


fig = go.Figure()
fig.add_trace(go.Choropleth(
    z = all_dogs_breed_by_zipcode_income_geoms.median_income,
    geojson=geojson,
    locations=all_dogs_breed_by_zipcode_income_geoms.zipcode,
    locationmode="geojson-id",
    featureidkey="properties.zcta",
    colorscale="viridis",
    #color_continuous_scale='Picnic',
#    zmid=0,
    visible=True,
    colorbar=dict(
        x=0,
        xref="container",
        title="Median Income"
    )
    ))
fig.add_trace(go.Choropleth(
    z = all_dogs_breed_by_zipcode_income_geoms[all_dogs_breed_by_zipcode_income_geoms["breedname"] == "Affenpinscher"].breed_pct_diff,
    geojson=geojson,
    locations=all_dogs_breed_by_zipcode_income_geoms[all_dogs_breed_by_zipcode_income_geoms["breedname"] == "Affenpinscher"].zipcode,
    locationmode="geojson-id",
    featureidkey="properties.zcta",
    colorscale="Picnic",
    zmid=0,
    visible=True,
    colorbar=dict(
        x=1,
        xref="container",
        title="Difference in Percentage<br>from City-Wide Average",
        tickformat = '.2%'
    )
    ))
fig.update_geos(fitbounds="geojson", visible=False)
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

fig.show()


# In[47]:


dogs_over_100_geoms = all_dogs_breed_by_zipcode_income_geoms.total_dogs_zipcode >=100

fig = go.Figure()
fig.add_trace(go.Choropleth(
    z = all_dogs_breed_by_zipcode_income_geoms.median_income,
    geojson=geojson,
    locations=all_dogs_breed_by_zipcode_income_geoms.zipcode,
    locationmode="geojson-id",
    featureidkey="properties.zcta",
    colorscale="viridis",
    visible=True,
    colorbar=dict(
        x=0,
        xref="container",
        title="Median Income"
    )
    ))

traces_list = []
button_list = []

button_list.append(
    dict(label="Median Income",
        method="update",
        args=[
            {"visible": [True] + [False] * len(repeat_breeds)},
            {"title": "Median Income"}
        ]))

for breed in repeat_breeds:
#    current_breed = all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].breedname == breed
    current_breed = all_dogs_breed_by_zipcode_income_geoms.breedname == breed
    trace_item = go.Choropleth(
		name = breed,
        z = all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].breed_pct_diff,
#        z = all_dogs_breed_by_zipcode_income_geoms[(current_breed)].breed_pct_diff,
        geojson=geojson,
        locations=all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].zipcode,
#        locations=all_dogs_breed_by_zipcode_income_geoms[(current_breed)].zipcode,
        locationmode="geojson-id",
        featureidkey="properties.zcta",
        colorscale="Picnic",
        zmid=0,
## don't set the max and min range, because some breeds are so limited as to appear all white
        # zmax=max_range,
        # zmin=-max_range,
        colorbar=dict(
            x=1,
            xref="container",
            title="Difference in Percentage<br>from City-Wide Average",
            tickformat = '.2%'
        ),
        visible = False
        #visible = True
        )
    traces_list.append(trace_item)
    new_button = dict(label=breed,
#        method="update",
        args=[
            {"visible": [True] + [False] * (len(repeat_breeds.tolist()))},
            {"title": f"{breed}"}]
            )
    new_button["args"][0]["visible"][repeat_breeds.tolist().index(breed) + 1] = True
    button_list.append(new_button)


# for button in button_list:
#     print(button["args"][0]["visible"])
# for button in button_list:
#     print(button)
#     break

count = 0
for trace in traces_list:
    fig.add_trace(trace)
    count += 1
    if count == 4:
        break

fig.update_layout(
    updatemenus = [dict(
        active = 0,
        buttons = button_list,
        # pad_r = 30
        y=1,
        x=.5
    )]
#    geo = dict(fitbounds = "geojson")
)

fig.update_geos(fitbounds="geojson", visible=False)

first_title = "<span style='align: right'>Median Income and<br>Breed Distribution</span>"
fig.update_layout(title=f"<b>{first_title}</b>",
                  title_x=0.75,
                  title_y=0.1,
                #   autosize=False,
                #   width=1000,
                #   height=1000
                  )

fig.update_layout(margin={"r":0,
                          "l":0,
                          "t":0,
                          "b":0})

fig.show()


# In[48]:


all_dogs_breed_by_zipcode_income_geoms["lon"] = all_dogs_breed_by_zipcode_income_geoms["geometry"].centroid.x
all_dogs_breed_by_zipcode_income_geoms["lat"] = all_dogs_breed_by_zipcode_income_geoms["geometry"].centroid.y
all_dogs_breed_by_zipcode_income_geoms.head(2)


# In[ ]:


dogs_over_100_geoms = all_dogs_breed_by_zipcode_income_geoms.total_dogs_zipcode >=100

fig = go.Figure()
fig.add_trace(go.Choropleth(
    z = all_dogs_breed_by_zipcode_income_geoms.median_income,
    geojson=geojson,
    locations=all_dogs_breed_by_zipcode_income_geoms.zipcode,
    locationmode="geojson-id",
    featureidkey="properties.zcta",
    marker_line_color="darkgrey",
    colorscale="Pinkyl",
    reversescale=True,
    visible=True,
    colorbar=dict(
        x=0,
        xref="container",
        title="Median Income",
        tickformat="$~s"
    )
    ))

traces_list = []
button_list = []

button_list.append(
    dict(label="Median Income",
        method="update",
        args=[
            {"visible": [True] + [False] * len(repeat_breeds)},
            {"title": "Median Income"}
        ]))

for breed in repeat_breeds:
    current_breed = all_dogs_breed_by_zipcode_income_geoms.breedname == breed
    size = all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].num_breed_zipcode/2
    sizeref = 2.*all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].num_breed_zipcode.max()/(20**2)
    trace_item = go.Scattergeo(
        name = breed,
        geojson=geojson,
        locations=all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].zipcode,
        locationmode="geojson-id",
        featureidkey="properties.zcta",
        lat=all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].lat,
        lon=all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].lon,
        mode = "markers",
        marker=dict(
            color = all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].breed_pct_diff,
            size=size,
            sizemode='area',
            sizeref=sizeref,
            sizemin=1,
            colorscale="PRGn",
            cmid=0,
            colorbar=dict(
                x=1,
                xref="container",
                title="Difference in Percentage<br>from City-Wide Average",
                tickformat = '.2%'
                ),
            ),
        customdata = np.stack((all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].num_breed_zipcode, 
                               all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].zipcode,
                               all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].median_income,
                               all_dogs_breed_by_zipcode_income_geoms[(current_breed) & (dogs_over_100_geoms)].breed_pct_zipcode), axis=-1),
        hovertemplate='Median Income: %{customdata[2]:$.0f}<br>' +
            '% Breed in Zip Code: %{customdata[3]:.2%}<br>' +
            'Number of Breed in Zip Code: %{customdata[0]}<br>' +
            'Zip Code: %{customdata[1]}',
        visible = False
        #visible = True
        )
    traces_list.append(trace_item)
    new_button = dict(label=breed,
#        method="update",
        args=[
            {"visible": [True] + [False] * (len(repeat_breeds.tolist()))},
            {"title": f"{breed}"}]
            )
    new_button["args"][0]["visible"][repeat_breeds.tolist().index(breed) + 1] = True
    button_list.append(new_button)


# for button in button_list:
#     print(button["args"][0]["visible"])
# for button in button_list:
#     print(button)
#     break

count = 0
for trace in traces_list:
    fig.add_trace(trace)
    count += 1
    if count == 50:
        break

fig.update_layout(
    updatemenus = [dict(
        active = 0,
        buttons = button_list,
        # pad_r = 30
        y=1,
        x=.5
    )]
#    geo = dict(fitbounds = "geojson")
)

fig.update_geos(fitbounds="geojson", visible=False)

first_title = "<span style='align: right'>Median Income and<br>Breed Distribution</span>"
fig.update_layout(title=f"<b>{first_title}</b>",
                  title_x=0.75,
                  title_y=0.1,
                #   autosize=False,
                #   width=1000,
                #   height=1000
                  )

fig.update_layout(margin={"r":0,
                          "l":0,
                          "t":0,
                          "b":0})

#fig.show()
#first_title # random text instead of giant image file


# In[ ]:




