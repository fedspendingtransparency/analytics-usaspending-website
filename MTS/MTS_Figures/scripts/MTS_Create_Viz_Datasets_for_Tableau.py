"""
Author: Janelle Becker

GOALS OF THIS SCRIPT:
    --Read in the current MTS excel file 
    --Create a dataset for Figure 1, Figure 3, and Figure 4 of the MTS
    --Get the data in a format that is ready for data visualization 
        in Tableau, which includes
            * LONG datasets without formatting
            * Converting numbers actual, not scaled, values (Tableau can then scale back to $M or $B)
    
    --The cover figure can be powered by Table 9, but is in a separate script
    --Figure 1 can be powered by Table 1
    --Figure 3 can be powered by Table 9
    --Figure 4 can be powered by Table 9

        


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
import os
import xlrd

starttime = time.time()

### SET UP THE DIRECTORIES ----------------------------------------------------

"""CHANGE ME! Each user will need to update to match their own directory."""

main_dir = "C:/Users/jbecke09/Documents/DATA Act"
project_dir = main_dir + "/MTS_JMB_Only"
data_dir = project_dir + "/data"
monthly_dir = data_dir + "/raw/monthly"
output_dir = data_dir + "/output"
df9_dir = output_dir + "/table9"
masters_dir = df9_dir + "/masters/"



os.chdir(monthly_dir) #change working directory to where the MTS are located 
os.listdir(os.getcwd()) #list out files in there as a sanity check 

###############################################################################
###############################################################################
###############################################################################
#%%
"""|--------------------------------------------------------------------|"""
"""| INPUT CURRENT/PREV MONTH AND YEAR HERE                             |"""
"""|--------------------------------------------------------------------|"""

"""CHANGE ME! You will need to change this monthly."""

curr_mo = "01" # Use two digits
curr_fy =    "17" # Use two digits

prev_mo = "12"  # Use two digits
prev_fy = "16"  # Use two digits

path = monthly_dir + "/mts0117.xls" #replace the new MTS file name 



"""CHANGE ME! Go clear the table9 folder (MTS > data > output > table9) by moving any files to archived"""

###############################################################################
###############################################################################
###############################################################################



#%%
"""|--------------------------------------------------------------------------|"""
"""|--STEP 1: MAKE DATAFRAMES FOR EACH VISUALIZATION: Table 9 --> Cover Fig   |"""
"""|--------------------------------------------------------------------------|"""

# Cover Figure requires Table 9 ==> read that in and clean it up 

"""|--------------------------------------------------------------------|"""
"""|--STEP 1a: READ IN THE DATA - Table 9 (Sources/Functions)           |"""
"""|--------------------------------------------------------------------|"""
# Table 9 gives Source and Function for Receipts/Outlays

whatiwant = {col: str for col in (0,3)}
df9 = pd.read_excel(path, 
                   sheetname="Table 9", 
                   header=2, 
                   converters=whatiwant)


"""|--------------------------------------------------------------------|"""
"""|--STEP 1b: WRANGLE THE DATA - Table 9                               |"""
"""|--------------------------------------------------------------------|"""

# Remove whitespace in column names -------------------------------------------
df9.columns.tolist() #oh it's a newline that was causing problems 

# Rename columns ---------------------------------------------------------------
cols_df9 = df9.columns.tolist()
rename_columns = [
        'source_func', #source or function categorical  variable
        'amt', #amount variable, regardless of R/O
        'fytd', #fytd amount 
        'comp_per_pfy'] #previous comparable period amount
for (oldcolname, replacement) in zip(cols_df9, rename_columns):
     df9.rename(columns={oldcolname : replacement}, inplace=True)
df9.columns.tolist()    #check that it went right

# Add in year and month since it wasn't a part of this table anywhere but the title
df9['fy'] = ""
df9['fy'] = "20" + str(path[-6:-4])
df9['month'] = ""
df9['month'] = str(path[-8:-6])

# Create a column indicating if it's a receipt or an outlay -----------------

# Create columns 
df9['rec'] = False
df9['outlay'] = False

# Figure out the indices for where it's labeled receipt/outlay as a "header" in the excel file

    # Outlays header ndex value
bool_vector = df9.loc[:,'source_func'] == "Net Outlays" #these three lines are condensed to one later on 
index_out = df9[bool_vector].index.tolist()
index_out = index_out[0]

# Make it true if receipt, true if outlay
for i in range(0,index_out):
    df9['rec'][i] = True

for i in range(index_out, len(df9)):
    df9['outlay'][i] = True

# Drop the rows with just receipt/outlay in them as headers and the final note ----------
df9.drop(df9.index[[0, index_out]], inplace=True)
df9 = df9[(df9['source_func'] != ". Note: Details may not add to totals due to rounding.")]
df9.reset_index(drop=True, inplace=True)


# Strip whitespace from source_func column ----------------------------------
df9['source_func'] = df9['source_func'].str.strip()
df9['source_func'] = df9['source_func'].astype(str)

# Unnest by creating a category variable---------------------------------------

# Make new column based on old column 
df9['source_func_parent'] = df9['source_func'] #making a source/function PARENT variable 

# Find the index value for where this is true
bool_vector = df9.loc[:,'source_func'] == "Employment and General Retirement"
index_EGR = df9[bool_vector].index.tolist()
index_EGR = index_EGR[0]

bool_vector = df9.loc[:,'source_func'] == "Unemployment Insurance"
index_UI = df9[bool_vector].index.tolist()
index_UI = index_UI[0]

bool_vector = (df9.loc[:,'source_func'] == "Other Retirement") | (df9.loc[:,'source_func'] == "OtherRetirement")
index_OR = df9[bool_vector].index.tolist()
index_OR = index_OR[0]

index_SIRR = (index_EGR - 1)

# Rename those cells
df9['source_func_parent'][index_EGR] = "Social Insurance and Retirement Receipts"
df9['source_func_parent'][index_UI] = "Social Insurance and Retirement Receipts"
df9['source_func_parent'][index_OR] = "Social Insurance and Retirement Receipts"


df9.drop(df9.index[index_SIRR], inplace=True)
df9.reset_index(drop=True, inplace=True)

# Convert numbers from str to int ----------------------------------------------

# remove all commas
df9['amt'] = df9['amt'].str.replace(',', '')
df9['fytd'] = df9['fytd'].str.replace(',', '')
df9['comp_per_pfy'] = df9['comp_per_pfy'].str.replace(',', '')

# make an integer
df9['amt'] = df9['amt'].astype(float)
df9['fytd'] = df9['fytd'].astype(float)
df9['comp_per_pfy'] = df9['comp_per_pfy'].astype(float)


### Add in S/D so we can simply use Table 9 instead of merge 
    # to power the cover figure -----------------------------------------------

index_tot_rec = df9[(df9.loc[:, 'source_func']=="Total") & (df9.loc[:, 'rec']==True)].index.tolist()[0]     #this line replaces the three line version from above 
index_tot_out = df9[(df9.loc[:, 'source_func']=="Total") & (df9.loc[:, 'outlay']==True)].index.tolist()[0]

deficit_mo = -1*(df9['amt'][index_tot_rec] - df9['amt'][index_tot_out])     #negative deficit --> surplus
deficit_fytd = -1*(df9['fytd'][index_tot_rec] - df9['fytd'][index_tot_out]) #negative deficit --> surplus

if deficit_mo < 0: #if we have a monthly surplus
    month_def_surp_label = "surplus for the month"
elif deficit_mo > 0: #if we have a monthly deficit
    month_def_surp_label = "deficit for the month"
    
if deficit_fytd < 0: #if we have a yearly surplus
    year_def_surp_label = "surplus fytd"
elif deficit_fytd > 0: #if we have a monthly deficit
    year_def_surp_label = "deficit fytd"

month_def_surp_label
year_def_surp_label

#Ensure sourece_parent_func represents surplus/deficit correctly
if month_def_surp_label.startswith("deficit"):
    month_def_surp_source_func_parent = "Deficit"
else:
    month_def_surp_source_func_parent = "Surplus"

if year_def_surp_label.startswith("deficit"):
    year_def_surp_source_func_parent = "Deficit"
else:
    year_def_surp_source_func_parent = "Surplus"

month_def_surp_source_func_parent
year_def_surp_source_func_parent

#Now because the labels indicate whether its surplus or deficit, make the values positive 

temp = pd.DataFrame( [[month_def_surp_label, np.abs(deficit_mo), 0, 0, "20" + str(path[-6:-4]), str(path[-8:-6]),False,False,month_def_surp_source_func_parent],
                     [year_def_surp_label, 0, np.abs(deficit_fytd),0,"20" + str(path[-6:-4]),str(path[-8:-6]),False,False, year_def_surp_source_func_parent]],
    columns = ['source_func', 'amt', 'fytd', 'comp_per_pfy', 
               'fy', 'month', 'rec', 'outlay','source_func_parent'])



df9 = pd.concat([df9, temp], axis=0)
df9.reset_index(drop=True, inplace=True)




### Create dataframe for the figure ------------------------------------------
df_fig_cov = df9.copy()

#%%
"""|--------------------------------------------------------------------|"""
"""|--STEP 1c: Iterating in order to create stuff in Tableau            |"""
"""|--------------------------------------------------------------------|"""

# Create a column to create the bars I want to see
df_fig_cov['amount_type'] = ""
for i in range(len(df_fig_cov)):
    if df_fig_cov['rec'][i]==True:
        df_fig_cov['amount_type'][i] = "Receipt"
    elif df_fig_cov['outlay'][i]==True:
        df_fig_cov['amount_type'][i] = "Outlay"
    else:
        df_fig_cov['amount_type'][i] = df_fig_cov['source_func_parent'][i]

# Find index value for totals
index_tot_rec = df_fig_cov[(df_fig_cov.loc[:, 'source_func']=="Total") & (df_fig_cov.loc[:, 'rec']==True)].index.tolist()[0]
index_tot_out = df_fig_cov[(df_fig_cov.loc[:, 'source_func']=="Total") & (df_fig_cov.loc[:, 'outlay']==True)].index.tolist()[0]

# Use that index value to rename some value for another column 
df_fig_cov['amount_type'][index_tot_rec] = "Total Receipts"
df_fig_cov['amount_type'][index_tot_out] = "Total Outlays"

df_fig_cov['source_func_parent'][index_tot_rec] = "Total Receipts"
df_fig_cov['source_func_parent'][index_tot_out] = "Total Outlays"

### Deficit/Surplus popping up in either Receipts/Outlays----------------------

#If receipts < outlays, then label deficit as total receipts 
    #so it'll pop up in that column in tableau

#MONTHLY SURPLUS/DEFICIT ROW
# If deficit, make amount type Total Receipts (parent was set above as deficit)
index_month_def_surp = df_fig_cov[(df_fig_cov.loc[:, 'source_func']==month_def_surp_label)].index.tolist()[0]

if df_fig_cov['source_func_parent'][index_month_def_surp]=="Surplus":
    df_fig_cov['amount_type'][index_month_def_surp] = "Total Outlays"
elif df_fig_cov['source_func_parent'][index_month_def_surp]=="Deficit":
    df_fig_cov['amount_type'][index_month_def_surp] = "Total Receipts"
else:
    print("Error!")




# FYTD SURPLUS/DEFICIT ROW

#Find index for deficit FYTD value
index_fytd_def_surp = df_fig_cov[(df_fig_cov.loc[:, 'source_func']==year_def_surp_label)].index.tolist()[0]

if df_fig_cov['source_func_parent'][index_fytd_def_surp]=="Surplus":
    df_fig_cov['amount_type'][index_fytd_def_surp] = "Total Outlays"
elif df_fig_cov['source_func_parent'][index_fytd_def_surp]=="Deficit":
    df_fig_cov['amount_type'][index_fytd_def_surp] = "Total Receipts"
else:
    print("Error!")




#%%
"""|--------------------------------------------------------------------------|"""
"""|--STEP 2: MAKE df's TO DRIVE THE VISUALIZATIONS: Table 1-->Fig 1          |"""
"""|--------------------------------------------------------------------------|"""
 # Figure 1 requires Table 1 ==> read that in and clean it up 


"""|--------------------------------------------------------------------|"""
"""|--STEP 2a: READ IN THE DATA - Table 1                               |"""
"""|--------------------------------------------------------------------|"""
# Table 1 gives receipts, outlays, and surplus/deficit for previous and current FYs

path #this was set above
whatiwant = {col: str for col in (0,3)}
df1 = pd.read_excel(path, 
                   sheetname="Table 1", 
                   header=2, 
                   skiprows=[3, 18], 
                   converters=whatiwant)


"""|--------------------------------------------------------------------|"""
"""|--STEP 2b: WRANGLE THE DATA - Table 1                               |"""
"""|--------------------------------------------------------------------|"""

### Create a Fiscal Year Column -----------------------------------------------
df1.rename(columns ={'Period': 'month'}, inplace = True)
df1.rename(columns ={'Receipts': 'recpt'}, inplace = True)
df1.rename(columns ={'Outlays': 'outlay'}, inplace = True)
df1.rename(columns ={'Deficit/Surplus (-)': 'deficit'}, inplace = True)

previous_fy = df1['month'][0]
previous_fy = previous_fy[3:]
current_fy = str(int(previous_fy) + 1)

df1['fy'] = ""

for i in range(1,14): # this assumes they always list the FY XXXX in the corner, and  include year-to-date line as well
    df1['fy'][i] = previous_fy

for i in range(14, len(df1)):
    df1['fy'][i] = current_fy


### Drop rows that aren't months ----------------------------------------------
    # Didn't know how to drop all possible future notes, so i just kept all months 
df1['month'] = df1['month'].str.strip()
df1['month'] = df1['month'].astype(str)
# turn them to lower 
for i in range(len(df1)):
    df1['month'][i] = df1['month'][i].lower()

# MONTHS
keep = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"]
keep2 = []
for month in keep:
    keep2.append(month.lower())
    
df1['data_type'] = ""
df1.loc[df1['month'].isin(keep2), 'data_type'] = "real"

# NOTES
df1.loc[~df1['month'].isin(keep2), 'data_type'] = "notes_or_other"
df1_notes_or_other = df1[df1['data_type'] == 'notes_or_other']


df1 = df1[df1['data_type'] == 'real']
del df1['data_type']
df1.reset_index(drop=True, inplace=True)

### Add in cumulative sum columns ----------------------------------------------

# remove all commas
df1['recpt'] = df1['recpt'].str.replace(',', '')
df1['outlay'] = df1['outlay'].str.replace(',', '')
df1['deficit'] = df1['deficit'].str.replace(',', '')

# make an integer
df1['recpt'] = df1['recpt'].astype(float)
df1['outlay'] = df1['outlay'].astype(float)
df1['deficit'] = df1['deficit'].astype(float)

# Create cum sum columns ----------------------------------------------------

# First, split by year
g = df1[(df1['fy'] != current_fy)] #this is just 2016
h = df1[(df1['fy'] != previous_fy)] #this is just 2017


#Next, find cumulative sum for that year 
g['recpt_ytd'] = g['recpt'].cumsum()
g['outlays_ytd'] = g['outlay'].cumsum()
g['deficit_ytd'] = g['deficit'].cumsum()

h['recpt_ytd'] = h['recpt'].cumsum()
h['outlays_ytd'] = h['outlay'].cumsum()
h['deficit_ytd'] = h['deficit'].cumsum()

#Put them back together

df1 = pd.concat([g, h], axis=0, ignore_index=True)



### Create dataframe for the figure (R/O only) -------------------------------
df_fig1 = df1[['month',
               'fy',
               'recpt',
               'outlay',
               'deficit']]
#%% 
"""|--------------------------------------------------------------------|"""
"""|--STEP 2c: Iterating in order to create stuff in Tableau            |"""
"""|--------------------------------------------------------------------|"""

### Create a negative version of outlays so Tableau can plot below x-axis -----------------
df_fig1['neg_outlays'] = -1*df_fig1['outlay']


### Create a categorical variable for coloring purposes in Tableau ------------
df_fig1['amt_type'] = ""


### Make long by having only one "amount" column 
    # and indicate type in separate variable------------------------------------

x = df_fig1[['month',
               'fy',
               'recpt',
               'amt_type']]
x['amt_type'] = "Receipt"



y = df_fig1[['month',
               'fy',
               'outlay',
               'amt_type']]

y['amt_type'] = "Outlay"



z = df_fig1[['month',
               'fy',
               'deficit',
               'amt_type']]
z['amt_type'] = "Deficit"


### Getting Tableau to display how we want it to ----------------------------------------------
# Have a negative outlays variables so that Tableau knows to plot it below x-axis
y['neg_outlays'] = -1*y['outlay']

x.rename(columns ={'recpt': 'amount_RO'}, inplace = True) #amount_receipt/outlay
y.rename(columns ={'neg_outlays': 'amount_RO'}, inplace = True) #amount_receipt/outlay
z.rename(columns ={'deficit': 'amount_DS'}, inplace = True) #amount_deficit/surplus


df_fig1 = pd.merge(x,z, on= ["month", "fy"], how='outer', indicator= "LxRz")
df_fig1.columns.tolist()

df_fig1 = df_fig1[['month', 'fy', 'amount_RO', 'amt_type_x', 'amount_DS']]
df_fig1.rename(columns ={'amt_type_x': 'amt_type'}, inplace = True)
df_fig1 = pd.concat([df_fig1, y], axis=0, ignore_index=True)

del df_fig1['outlay']

### Add date column that combines month and fiscal year for Tableau -----------

df_fig1['date'] = df_fig1['month'] + ", " + df_fig1['fy'] 


### Make deficit numbers negative so Tableau knows how to plot them
df_fig1['deficit_as_neg'] = -1*df_fig1['amount_DS']







#%%
"""|--------------------------------------------------------------------------|"""
"""|--STEP 3: MAKE df's TO DRIVE THE VISUALIZATIONS: Table 9's --> Fig 3, 4   |"""
"""|--------------------------------------------------------------------------|"""

# Figure 4 needs outlays for Fy16 and FY17 by FUNCTION 
    # i.e. Table 9 from the past 12-24 MTS's

### INPUT CURRENT/PREV MONTH AND YEAR  HERE -------------------------------

os.chdir(monthly_dir) #change working directory to where these MTS files are stored
os.listdir(os.getcwd()) #list out files in there 



"""|--------------------------------------------------------------------|"""
"""|--STEP 3a: GET LIST OF FILENAMES FOR WRANGLING Table 9's            |"""
"""|--------------------------------------------------------------------|"""

### We want every file that ends "16" and any file ending in 17 up to this month

# Get every file name ending with "17" AND "16" into a list

list_of_files_CFY = [filename for filename in os.listdir('.') if filename.endswith(curr_fy + ".xls")]
list_sans_later_months = [item for item in list_of_files_CFY if int(item[3:5]) <= int(curr_mo)]
list_of_files_PFY = [filename for filename in os.listdir('.') if filename.endswith(prev_fy + ".xls")]
list_both_fy = list_sans_later_months + list_of_files_PFY
list_both_fy

if prev_mo == "12":
    list_both_fy = list_of_files_PFY
else:
    list_both_fy = list_both_fy

list_both_fy   
   


"""|--------------------------------------------------------------------|"""
"""|--STEP 3b: RUN EACH FILE THROUGH CLEANING PROCESS & SAVE AS CSV     |"""
"""|--------------------------------------------------------------------|"""

for filename in list_both_fy:
    # Table 9 gives Source and Function for Receipts/Outlays
    path = monthly_dir + "/" + str(filename)
    whatiwant = {col: str for col in (0,3)}
    df9 = pd.read_excel(path, 
                       sheetname="Table 9", 
                       header=2, 
                       converters=whatiwant)
   
    # Remove whitespace in column names -------------------------------------------
    df9.columns.tolist() #oh it's a newline
    
    #Rename columns ---------------------------------------------------------------
    cols_df9 = df9.columns.tolist()
    rename_columns = [
            'source_func', 
            'amt',
            'fytd',
            'comp_per_pfy']
    for (oldcolname, replacement) in zip(cols_df9, rename_columns):
         df9.rename(columns={oldcolname : replacement}, inplace=True)
    df9.columns.tolist()    #check that it went right
    
    ### Add in year and month since it wasn't a part of this table anywhere but the title
    df9['fy'] = ""
    df9['fy'] = "20" + str(filename[5:7])
    df9['month'] = ""
    df9['month'] = str(filename[3:5])
    
    ### Create a column indicating if it's a receipt or an outlay -----------------
    
    # Create columns 
    df9['rec'] = False
    df9['outlay'] = False
    
        # Outlays index value
    bool_vector = df9.loc[:,'source_func'] == "Net Outlays"
    index_out = df9[bool_vector].index.tolist()
    index_out = index_out[0]
    
    # Make it true if receipt, true if outlay
    for i in range(0,index_out):
        df9['rec'][i] = True
    
    for i in range(index_out, len(df9)):
        df9['outlay'][i] = True
    
    # Drop the rows with just receipt/outlay in them and the final note ----------
    df9.drop(df9.index[[0, index_out]], inplace=True)
    df9 = df9[(df9['source_func'] != ". Note: Details may not add to totals due to rounding.")]
    df9.reset_index(drop=True, inplace=True)
    
    
    ### Strip whitespace from source_func column ----------------------------------
    df9['source_func'] = df9['source_func'].str.strip()
    df9['source_func'] = df9['source_func'].astype(str)
    
    # Unnest by creating a category variable
        #(instead of renaming, which I tried before) ------------------------------
    
    #Make new column based on old column 
    df9['source_func_parent'] = df9['source_func']
    
    # Find the index value for where this is true
    bool_vector = df9.loc[:,'source_func'] == "Employment and General Retirement"
    index_EGR = df9[bool_vector].index.tolist()
    index_EGR = index_EGR[0]
    
    bool_vector = df9.loc[:,'source_func'] == "Unemployment Insurance"
    index_UI = df9[bool_vector].index.tolist()
    index_UI = index_UI[0]
    
    bool_vector = (df9.loc[:,'source_func'] == "Other Retirement") | (df9.loc[:,'source_func'] == "OtherRetirement")
    index_OR = df9[bool_vector].index.tolist()
    index_OR = index_OR[0]
    
    index_SIRR = (index_EGR - 1)
    
    # Rename those cells
    df9['source_func_parent'][index_EGR] = "Social Insurance and Retirement Receipts"
    df9['source_func_parent'][index_UI] = "Social Insurance and Retirement Receipts"
    df9['source_func_parent'][index_OR] = "Social Insurance and Retirement Receipts"
    
    
    df9.drop(df9.index[index_SIRR], inplace=True)
    df9.reset_index(drop=True, inplace=True)
    
    #Convert numbers from str to int ----------------------------------------------
    
    # remove all commas
    df9['amt'] = df9['amt'].str.replace(',', '')
    df9['fytd'] = df9['fytd'].str.replace(',', '')
    df9['comp_per_pfy'] = df9['comp_per_pfy'].str.replace(',', '')
    
    # (**) is a value below $500,000 and we dont know what it is, so.... zero
    df9['amt'] = df9['amt'].str.replace('\(\*\*\)', '0') #* is a special character in regex, you have to escape it: regex=False gave me error
    df9['fytd'] = df9['fytd'].str.replace('\(\*\*\)', '0')
    df9['comp_per_pfy'] = df9['comp_per_pfy'].str.replace('\(\*\*\)', '0')
    
    # make an integer
    df9['amt'] = df9['amt'].astype(float)
    df9['fytd'] = df9['fytd'].astype(float)
    df9['comp_per_pfy'] = df9['comp_per_pfy'].astype(float)
    
    
    ### Add in S/D so we can simply use Table 9 instead of merge 
        # to power the cover figure -----------------------------------------------
    
    
    index_tot_rec = df9[(df9.loc[:, 'source_func']=="Total") & (df9.loc[:, 'rec']==True)].index.tolist()[0]
    index_tot_out = df9[(df9.loc[:, 'source_func']=="Total") & (df9.loc[:, 'outlay']==True)].index.tolist()[0]
    
    deficit_mo = -1*(df9['amt'][index_tot_rec] - df9['amt'][index_tot_out])
    deficit_fytd = -1*(df9['fytd'][index_tot_rec] - df9['fytd'][index_tot_out])
    
    
    temp = pd.DataFrame( [["deficit for the month", deficit_mo, 0, 0, "20" + str(path[-6:-4]), str(path[-8:-6]),False,False,"Deficit"],
                         ["deficit fytd", 0, deficit_fytd,0,"20" + str(path[-6:-4]),str(path[-8:-6]),False,False,"Deficit"]],
        columns = ['source_func', 'amt', 'fytd', 'comp_per_pfy', 
                   'fy', 'month', 'rec', 'outlay','source_func_parent'])
    
    
    
    df9 = pd.concat([df9, temp], axis=0)
    df9.reset_index(drop=True, inplace=True)
    
    title = filename[:-4]
   
    ### Write to CSV --------------------------------------------------------------
    df9.to_csv(df9_dir + "/df9_from_" + str(title) + ".csv", index=False, header=True)



#%%
"""|--------------------------------------------------------------------|"""
"""|--STEP 3c: Create master df9 for current month                      |"""
"""|--------------------------------------------------------------------|"""

os.chdir(df9_dir) #change working directory to where all the df9's are stored
os.listdir(os.getcwd()) #list out files in there 

#Put all filepaths by report into a list 
df9_filename_list = [filename for filename in os.listdir('.')]

list_of_dfs = [pd.read_csv(x, index_col=0, encoding = 'latin1') for x in df9_filename_list if x.startswith("df9")]

 # For longer datasets, this can hit performance hard, so be efficient with RAM/memory
 # and do 2 at a time then delete 

df9_master = list_of_dfs[0]
starting_index = len(list_of_dfs)-1
for i in range(starting_index, 0, -1):
    df9_master = pd.concat([df9_master, list_of_dfs[i]], ignore_index = True)
    del list_of_dfs[i]




### Write to CSV --------------------------------------------------------------
df9_master.to_csv(masters_dir + "/master_df9_" + str(curr_mo) + str(curr_fy) + ".csv", index=False, header=True)



#%%
"""|--------------------------------------------------------------------|"""
"""|--STEP 4: MAKE df's TO DRIVE THE VISUALIZATIONS: Table 9-->Fig 3    |"""
"""|--------------------------------------------------------------------|"""

### Create dataframe for the figure (YTD only) -------------------------------
df9_master.columns.tolist()

df_fig3 = df9_master[['fy',
                      'month',
                      'amt',
                      'rec',
                      'outlay',
                      'source_func_parent']]

# keep row if value in "receipts" column equals true
# Drop row if value in "receipts" column equals false ---------------------------
df_fig3 = df_fig3[df_fig3['rec']==True]

# Drop column by name - drop outlay col, rec col 
del df_fig3['rec']
del df_fig3['outlay']

# Create a date column for Tableau to turn into one date (e.g. 10/1/16)--------
df_fig3['date'] = ""
df_fig3['month'] = df_fig3['month'].astype(str)
df_fig3['fy'] = df_fig3['fy'].astype(str)
df_fig3['date'] = df_fig3['month'] + "-" + df_fig3['fy']  

# Rename column ---------------------------------------------------------------
df_fig3.rename(columns= {"amt" : "receipt_amount"}, inplace=True)

# Reset index
df_fig3.reset_index(drop=True, inplace=True)





#%%
"""|--------------------------------------------------------------------|"""
"""|--STEP 5: MAKE df's TO DRIVE THE VISUALIZATIONS: Table 9-->Fig 4    |"""
"""|--------------------------------------------------------------------|"""

### Create dataframe for the figure ------------------------------------------
df_fig4 = df9_master[['fy',
                      'month',
                      'amt',
                      'rec',
                      'outlay',
                      'source_func_parent']]



# Keep row if value in "outlays" column equals true ---------------------------
df_fig4 = df_fig4[df_fig4['outlay']==True]

# Drop column by name - drop outlay col, rec col 
del df_fig4['rec']
del df_fig4['outlay']



# Create a date column for Tableau to turn into one date (e.g. 10/1/16)--------
df_fig4['date'] = ""
df_fig4['month'] = df_fig4['month'].astype(str)
df_fig4['fy'] = df_fig4['fy'].astype(str)
df_fig4['date'] = df_fig4['month'] + "-" + df_fig4['fy']  

# Rename column ---------------------------------------------------------------
df_fig4.rename(columns= {"amt" : "outlay_amount"}, inplace=True)

# Reset index
df_fig4.reset_index(drop=True, inplace=True)


df_fig4 = df_fig4[df_fig4['source_func_parent'] != ". (**) Less than absolute value of $500,000"]


#%%
"""|--------------------------------------------------------------------|"""
"""|--STEP 6: CLEANING: NOTES AND $1M --> 1,000,000                     |"""
"""|--------------------------------------------------------------------|"""

"""
Clean up notes
Convert scaled numbers back to, e.g. millions

"""
# COVER FIGURE ---------------------------------------------------------------
df_fig_cov.columns.tolist()

# Rename the old columns as ($M)
df_fig_cov.rename(columns ={'amt': 'amt_M'}, inplace = True)
df_fig_cov.rename(columns ={'fytd': 'fytd_M'}, inplace = True)
df_fig_cov.rename(columns ={'comp_per_pfy': 'comp_per_pfy_M'}, inplace = True)

# Create columns with absolute numbers
df_fig_cov['amt'] = df_fig_cov['amt_M']*1000000
df_fig_cov['fytd'] = df_fig_cov['fytd_M']*1000000
df_fig_cov['comp_per_pfy'] = df_fig_cov['comp_per_pfy_M']*1000000


# FIGURE 1, v10a-------------------------------------------------------------------
df_fig1.columns.tolist()

# Rename the old columns as ($M)
df_fig1.rename(columns ={'amount_DS': 'amount_DS_M'}, inplace = True)
df_fig1.rename(columns ={'amount_RO': 'amount_RO_M'}, inplace = True)
df_fig1.rename(columns ={'deficit_as_neg': 'deficit_as_neg_M'}, inplace = True)

# Create columns with absolute numbers
df_fig1['amount_DS'] = df_fig1['amount_DS_M']*1000000
df_fig1['amount_RO'] = df_fig1['amount_RO_M']*1000000
df_fig1['deficit_as_neg'] = df_fig1['deficit_as_neg_M']*1000000
           

df_fig1.columns.tolist()

df_fig1 = df_fig1[['date',
  'amt_type',
 'amount_DS',
 'amount_RO',
 'deficit_as_neg']]





# FIGURE 3 -------------------------------------------------------------------
df_fig3.columns.tolist()

# Rename the old columns as ($M)
df_fig3.rename(columns ={'receipt_amount': 'receipt_amount_M'}, inplace = True)

# Create columns with absolute numbers
df_fig3['receipt_amount'] = df_fig3['receipt_amount_M']*1000000

# FIGURE 4 -------------------------------------------------------------------
df_fig4.columns.tolist()

# Rename the old columns as ($M)
df_fig4.rename(columns ={'outlay_amount': 'outlay_amount_M'}, inplace = True)

# Create columns with absolute numbers
df_fig4['outlay_amount'] = df_fig4['outlay_amount_M']*1000000
       
          
#%%          
"""|--------------------------------------------------------------------|"""
"""|--STEP 7: CLEANING: FIX SPACING ISSUES                              |"""
"""|--------------------------------------------------------------------|"""
# Things were popping up, e.g. CustomsDuties and Customs Duties. We won't want
# these to appear as two distinct categories. 

### Figure 3 -----------------------------------------------------------------
df_fig3['source_func_parent'].unique().tolist()

# Sources
df_fig3['source_func_parent_2'] = ""
df_fig3['source_func_parent_2'] = np.where(df_fig3['source_func_parent'].str.contains("Individual"), 'Individual Income Taxes', df_fig3['source_func_parent'])
df_fig3['source_func_parent_2'] = np.where(df_fig3['source_func_parent'].str.contains("Corporation"), 'Corporation Income Taxes', df_fig3['source_func_parent_2'])
df_fig3['source_func_parent_2'] = np.where(df_fig3['source_func_parent'].str.contains("Insurance"), 'Social Insurance and Retirement Receipts', df_fig3['source_func_parent_2'])
df_fig3['source_func_parent_2'] = np.where(df_fig3['source_func_parent'].str.contains("Estate"), 'Estate and Gift Taxes', df_fig3['source_func_parent_2'])
df_fig3['source_func_parent_2'] = np.where(df_fig3['source_func_parent'].str.contains("Excise"), 'Excise Taxes', df_fig3['source_func_parent_2'])
df_fig3['source_func_parent_2'] = np.where(df_fig3['source_func_parent'].str.contains("Customs"), 'Customs Duties', df_fig3['source_func_parent_2'])
df_fig3['source_func_parent_2'] = np.where(df_fig3['source_func_parent'].str.contains("Miscellaneous"), 'Miscellaneous Receipts', df_fig3['source_func_parent_2'])
df_fig3['source_func_parent_2'] = np.where(df_fig3['source_func_parent'].str.contains("Total"), 'Total', df_fig3['source_func_parent_2'])


len(df_fig3['source_func_parent_2'].unique().tolist())


del df_fig3['source_func_parent']
df_fig3.rename(columns ={'source_func_parent_2': 'source_func_parent'}, inplace = True)


### Figure 4 -----------------------------------------------------------------
df_fig4['source_func_parent'].unique().tolist()


# Functions 
df_fig4['source_func_parent_2']  = ""
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Defense"), 'National Defense', df_fig4['source_func_parent'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Affairs"), 'International Affairs', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Science"), 'General Science, Space, and Technology', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Resources"), 'Natural Resources and Environment', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Commerce"), 'Commerce and Housing Credit', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Regional"), 'Community and Regional Development', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Income"), 'Income Security', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Social"),'Social Security', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Veteran"), 'Veterans Benefits and Services', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Justice"), 'Administration of Justice', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Government"), 'General Government', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Interest"), 'Net Interest', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Undistributed"), 'Undistributed Offsetting Receipts', df_fig4['source_func_parent_2'])
df_fig4['source_func_parent_2'] = np.where(df_fig4['source_func_parent'].str.contains("Education"), 'Education, Training, Employment, and Social Services', df_fig4['source_func_parent_2'])


len(df_fig4['source_func_parent_2'].unique().tolist())


del df_fig4['source_func_parent']
df_fig4.rename(columns ={'source_func_parent_2': 'source_func_parent'}, inplace = True)




len(df_fig3['source_func_parent'].unique().tolist()) == 8 #including Total
len(df_fig4['source_func_parent'].unique().tolist()) == 20 #including Total

#%%

"""|--------------------------------------------------------------------|"""
"""|--STEP 8: CLEANING: ADD MONTHLY LABELS FOR TABLEAU                  |"""
"""|--------------------------------------------------------------------|"""

### Figure 3 ----------------------------------------------------------------
df_fig3.reset_index(drop=True, inplace=True)
df_fig3['total_R_month']=""

list_dates_df_fig3 = df_fig3['date'].unique().tolist()

for i in range(len(df_fig3)):
    
    for v in list_dates_df_fig3:
        index_tot_rec = df_fig3[(df_fig3['source_func_parent']=="Total") & (df_fig3['date']==str(v))].index.tolist()[0] 
        monthly_total_R = df_fig3['receipt_amount'][index_tot_rec]
    
        if df_fig3['date'][i]==str(v):    
            df_fig3['total_R_month'][i] = monthly_total_R


df_fig3['total_R_month'] = df_fig3['total_R_month'].astype(float)



### Figure 4 ----------------------------------------------------------------
df_fig4.reset_index(drop=True, inplace=True)
df_fig4.columns.tolist()
df_fig4['total_OL_month']=""

list_dates_df_fig4 = df_fig4['date'].unique().tolist()

for i in range(len(df_fig4)):
    
    for v in list_dates_df_fig4:
        index_tot_out = df_fig4[(df_fig4['source_func_parent']=="Total") & (df_fig4['date']==str(v))].index.tolist()[0] 
        monthly_total_OL = df_fig4['outlay_amount'][index_tot_out]
    
        if df_fig4['date'][i]==str(v):    
            df_fig4['total_OL_month'][i] = monthly_total_OL


df_fig4['total_OL_month'] = df_fig4['total_OL_month'].astype(float)


#%%
"""|--------------------------------------------------------------------|"""
"""|--STEP X: Clean up                                                  |"""
"""|--------------------------------------------------------------------|"""

del df9
del df1_notes_or_other
del df9_dir
del df9_filename_list




#%%
"""|--------------------------------------------------------------------|"""
"""|--STEP 9: WRITE THESE FIG DATASETS TO CSV                           |"""
"""|--------------------------------------------------------------------|"""
date = datetime.today().strftime("%y%m%d")

df_fig_cov.to_csv(output_dir + "/figure_datasets/fig_cover_" + str(curr_mo) + str(curr_fy) + "_made_" + str(date) + ".csv", index=False, header=True)

df_fig1.to_csv(output_dir + "/figure_datasets/fig1_" + str(curr_mo) + str(curr_fy) + "_made_" + str(date) + ".csv", index=False, header=True)

df_fig3.to_csv(output_dir + "/figure_datasets/fig3_" + str(curr_mo) + str(curr_fy) + "_made_" + str(date) + ".csv", index=False, header=True)

df_fig4.to_csv(output_dir + "/figure_datasets/fig4_" + str(curr_mo) + str(curr_fy) + "_made_" + str(date) + ".csv", index=False, header=True)

###############################################################################
###############################################################################
################################# END FILE ####################################
###############################################################################
###############################################################################

endtime = datetime.today()
end = time.time()
total_run_time_sec = (end-starttime) #yields seconds
total_run_time_min = int(total_run_time_sec/60)
total_run_time_hr = total_run_time_min/60 


print("***************      You started at " + str(starttime) + "      *********************")
print("***************      THE END!       " + str(endtime) + "      *********************")
print("")
print("***************      The total run time was " + str(total_run_time_min) + " minutes.        *************")
print("***************      The total run time was " + str(total_run_time_hr) + " hours.        *************")


