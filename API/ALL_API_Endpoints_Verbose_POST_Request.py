"""
GOALS OF THIS SCRIPT:
    -Call ALL USAspending API endpoints
    -Get verbose version
    -Convert to CSV 

"""
### Import stuff --------------------------------------------------------------
import pandas as pd
from datetime import datetime, timedelta
import time
import requests
import numpy as np
import json
import urllib
from pandas.io.json import json_normalize

start = time.time()

### SET UP THE DIRECTORIES ----------------------------------------------------
main_dir = "C:/Users/583902/Desktop/BAH1/_Treasury_DATA_Act/API stuff"
data_dir = main_dir + "/data"
output_dir = main_dir + "/output"

### Set up the URLs ----------------------------------------------------------
base_url = "https://api.usaspending.gov/api/v1/"
endpt_acct_awards = "accounts/awards/" #financial acct data grouped by award
endpt_awards = "awards/"
endpt_tas_balances = "tas/balances/"
endpt_tas_cats = "tas/categories/"
endpt_trans = "transactions/"
#endpt_fed_acct = "federal_accounts/"
#endpt_tas = "tas/"
#endpt_agency = "references/agency/"
#endpt_ref_cfda = "references/cfda/"

#default number of pages to return = 1
#default results per page = 100
#example for pagination and limits: /v1/awards/?page=5&limit=20 

################################################################################

#ACCOUNT AWARDS: --------------------------------------------------------------

   #POST
whatiwant = {"verbose" : "true"}

url = base_url + endpt_acct_awards
r = requests.post(url, data=whatiwant)
print(r.status_code, r.reason)
r.raise_for_status()
#r.headers
#r.request.headers

data = r.json() 
meta = data['page_metadata']
data = data['results']
df_acct_awards = pd.io.json.json_normalize(data) 



#Get every page and append that page to the main dataframe
i=2
while meta['has_next_page'] == True:
    print("Retreiving page " + str(i)) 
    r = requests.post(url + "?page=" + str(i) + "&limit=100", data=whatiwant)
    r.raise_for_status()
    data = r.json() 
    meta = data['page_metadata'] #page 2's meta data now 
    data = data['results']
    df_page = pd.io.json.json_normalize(data)
    df_acct_awards = pd.concat([df_acct_awards, df_page], axis=0)
    del df_page
    i = i + 1

cols_acct_awards_verbose = df_acct_awards.columns.tolist()

################################################################################
################################################################################

# AWARDS: --------------------------------------------------------------

   #POST
whatiwant = {"verbose" : "true"}

url = base_url + endpt_awards
r = requests.post(url, data=whatiwant)
print(r.status_code, r.reason)
r.raise_for_status()
#r.headers
#r.request.headers

data = r.json() 
meta = data['page_metadata']
data = data['results']
df_awards = pd.io.json.json_normalize(data) 



#Get every page and append that page to the main dataframe
i=2
while meta['has_next_page'] == True:
    print("Retreiving page " + str(i)) 
    r = requests.post(url + "?page=" + str(i) + "&limit=50", data=whatiwant)
    r.raise_for_status()
    data = r.json() 
    meta = data['page_metadata'] #page 2's meta data now 
    data = data['results']
    df_page = pd.io.json.json_normalize(data)
    df_awards = pd.concat([df_awards, df_page], axis=0)
    del df_page
    i = i + 1

cols_awards_verbose = df_awards.columns.tolist()



################################################################################
################################################################################

# TAS BALANCES --------------------------------------------------------------

   #POST
whatiwant = {"verbose" : "true"}

url = base_url + endpt_tas_balances
r = requests.post(url, data=whatiwant)
print(r.status_code, r.reason)
r.raise_for_status()
#r.headers
#r.request.headers

data = r.json() 
meta = data['page_metadata']
data = data['results']
df_tas_bal = pd.io.json.json_normalize(data) 



#Get every page and append that page to the main dataframe
i=2
while meta['has_next_page'] == True:
    print("Retreiving page " + str(i)) 
    r = requests.post(url + "?page=" + str(i) + "&limit=100", data=whatiwant)
    r.raise_for_status()
    data = r.json() 
    meta = data['page_metadata'] #page 2's meta data now 
    data = data['results']
    df_page = pd.io.json.json_normalize(data)
    df_tas_bal = pd.concat([df_tas_bal, df_page], axis=0)
    del df_page
    i = i + 1

cols_df_tas_bal_verbose = df_tas_bal.columns.tolist()

################################################################################
################################################################################

# TAS CATEGORIES --------------------------------------------------------------

   #POST
whatiwant = {"verbose" : "true"}

url = base_url + endpt_tas_cats
r = requests.post(url, data=whatiwant)
print(r.status_code, r.reason)
r.raise_for_status()
#r.headers
#r.request.headers

data = r.json() 
meta = data['page_metadata']
data = data['results']
df_tas_cats = pd.io.json.json_normalize(data) 



#Get every page and append that page to the main dataframe
i=2
while meta['has_next_page'] == True:
    print("Retreiving page " + str(i)) 
    r = requests.post(url + "?page=" + str(i) + "&limit=100", data=whatiwant)
    r.raise_for_status()
    data = r.json() 
    meta = data['page_metadata'] #page 2's meta data now 
    data = data['results']
    df_page = pd.io.json.json_normalize(data)
    df_tas_cats = pd.concat([df_tas_cats, df_page], axis=0)
    del df_page
    i = i + 1

cols_df_tas_cats_verbose = df_tas_cats.columns.tolist()

################################################################################
################################################################################

# TAS TRANSACTIONS --------------------------------------------------------------

   #POST
whatiwant = {"verbose" : "true"}

url = base_url + endpt_trans
r = requests.post(url, data=whatiwant)
print(r.status_code, r.reason)
r.raise_for_status()
#r.headers
#r.request.headers

data = r.json() 
meta = data['page_metadata']
data = data['results']
df_trans = pd.io.json.json_normalize(data) 



#Get every page and append that page to the main dataframe
i=2
while meta['has_next_page'] == True:
    print("Retreiving page " + str(i)) 
    r = requests.post(url + "?page=" + str(i) + "&limit=100", data=whatiwant)
    r.raise_for_status()
    data = r.json() 
    meta = data['page_metadata'] #page 2's meta data now 
    data = data['results']
    df_page = pd.io.json.json_normalize(data)
    df_trans = pd.concat([df_trans, df_page], axis=0)
    del df_page
    i = i + 1

cols_df_trans_verbose = df_trans.columns.tolist()

################################################################################





"""
############# WRITE THIS TO CSV ###############################################

#TAS CATEGORIES
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_tas_categories_" + str(date) + ".csv"
df_tas_cats.to_csv(path, index=False, header=True)


#ACCOUNT AWARDS
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_account_awards_through_page_7036_" + str(date) + ".csv"
df_acct_awards.to_csv(path, index=False, header=True)

#AGENCY - REFERENCES
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_agency_" + str(date) + ".csv"
df_agency.to_csv(path, index=False, header=True)

#AWARDS
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_awards_through_pg_1538_with_50_limit" + str(date) + "pages_1-120.csv"
df_awards.to_csv(path, index=False, header=True)

#FEDERAL ACCOUNTS
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_fed_account_" + str(date) + ".csv"
df_fed_acct.to_csv(path, index=False, header=True)

#CFDA - REFERENCES 
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_cfda_" + str(date) + ".csv"
df_ref_cfda.to_csv(path, index=False, header=True)

#TAS
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_tas_" + str(date) + ".csv"
df_tas.to_csv(path, index=False, header=True)

#TAS BALANCES
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_tas_bal_" + str(date) + ".csv"
df_tas_bal.to_csv(path, index=False, header=True)

#TRANSACTIONS
date = datetime.today().strftime("%m%d%y")
path = output_dir + "/API_pull_transactions_through_page_3824_" + str(date) + ".csv"
df_trans.to_csv(path, index=False, header=True)
"""



################################# END FILE #################################

end = time.time()
(end-start) #yields seconds
print("******************* The total run time was " + str(end - start) + " seconds. *************")

print("***************     THE END!        " + str(datetime.today()) + "      *********************")