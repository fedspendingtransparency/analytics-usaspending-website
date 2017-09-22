"""
Author: Janelle Becker
GOALS OF THIS SCRIPT:
    --Merge MTS receipt/outlay data (already wrangled from original MTS)
        with "model" data to have a t and path value
        for each source/function and then calculate the curve with that
    --Define the rank myself so it reflects what I want
    -
    - Add in spacer data
    - Add in fake category data to add as spacers for inner R/O pillars

"""

"""|--------------------------------------------------------------------|"""
"""|                         BRING IN THE DATA                          |"""
"""|--------------------------------------------------------------------|"""

### Import stuff ==============================================================
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
import timeit


start = time.time()
starttime = datetime.today()

#%%
### SET UP THE DIRECTORIES ----------------------------------------------------
"""CHANGE ME!"""

main_dir = "C:/Users/jbecke09/Desktop"
MTS_project_dir = 
sankey_project_dir = main_dir + "/MTS_Sankey" #sankey folder
data_dir = main_dir + "/MTS_JMB_Only/data/output/figure_datasets" #the output from the other scripts went here
output_dir = project_dir + "/data" #the output of this script will go here


"""CHANGE ME! You will need to change this monthly."""

# DEFICIT EXAMPLE
curr_mo = "08" # Use two digits
curr_fy =    "17" # Use two digits

prev_mo = "07"  # Use two digits
prev_fy = "16"  # Use two digits

""" The other script to wrangle MTS data must be run first.
    Filename: "MTS_Create_Viz_Datasets_for_Tableau.py" """
    
path = data_dir + "/fig_cover_0817_made_170922.csv"  # change the filename 



#%%
"""|--------------------------------------------------------------------|"""
"""|            CREATE SANKEY STARTER DATA                              |""" 
"""|--------------------------------------------------------------------|"""



### Read in the data ------------------------------------------------------
    # Read in the cover figure dataset from the previous script
df_fig_cov = pd.read_csv(path)
model =  pd.read_csv(output_dir + "/model.csv")

# Create a dataframe of just receepts/just outlays ---------------------------
df_receipts = df_fig_cov[(df_fig_cov['amount_type']=="Receipt") | (df_fig_cov['amount_type']=="Total Receipts")]
df_outlays = df_fig_cov[(df_fig_cov['amount_type']=="Outlay") | (df_fig_cov['amount_type']=="Total Outlays")]

# Remove the "total" line
df_receipts = df_receipts[df_receipts['source_func']!="Total"]
df_outlays = df_outlays[df_outlays['source_func']!="Total"]

# Note the current month/fiscal year of the file
current_month = str(df_fig_cov['fy'][0])
current_fy = str(df_fig_cov['month'][0])

if len(current_fy) == 1:
    current_fy = "0" + current_fy




# Drop columns----------------------------------------------------------------
    # Only keep the columns necessary for the sankey visualization:

# Receipts: Stage1_Source | Stage1_Source_Amount | Stage2_Receipt_Type | Link
# Outlays: Stage3_Outlay_Type | Stage3_Outlay_Amount | Stage4_Function | Link


df_receipts.columns.tolist()
df_rec = df_receipts[[
                     'source_func_parent',
                     'amt',
                     'amount_type']]


df_outlays.columns.tolist()
df_out = df_outlays[['amount_type',
                     'amt',
                     'source_func_parent']]
# Add link
df_rec['Link'] = "link"
df_out['Link'] = "link"

#Change names
df_rec.rename(columns ={'source_func_parent': 'Stage1_Source'}, inplace = True)
df_rec.rename(columns ={'amt': 'Stage1_Source_Amount'}, inplace = True)
df_rec.rename(columns ={'amount_type': 'Stage2_Receipt_Type'}, inplace = True)

df_out.rename(columns ={'amount_type': 'Stage3_Outlay_Type'}, inplace = True)
df_out.rename(columns ={'amt': 'Stage3_Outlay_Amount'}, inplace = True)
df_out.rename(columns ={'source_func_parent': 'Stage4_Function'}, inplace = True)


#Modify to have receipt type as receipt/deficit or outlay/surplus
    # The first script labeled things as "Receipt" or "Total Receipts" 
    # Or "Outlay" and "Total Outlays"
    # Let's change the language to be more explicit: Receipt/Deficit or Outlay/Surplus
df_rec.sort_values(by=['Stage1_Source_Amount'], ascending=False, inplace=True)
df_rec.reset_index(drop=True, inplace=True)

df_out.sort_values(by=['Stage3_Outlay_Amount'], ascending=False, inplace=True)
df_out.reset_index(drop=True, inplace=True)


for i in range(len(df_rec)):
    if df_rec['Stage2_Receipt_Type'][i] == "Total Receipts":
        df_rec['Stage2_Receipt_Type'][i] = "Deficit"

for i in range(len(df_out)):
    if df_out['Stage3_Outlay_Type'][i] == "Total Outlays":
        df_out['Stage3_Outlay_Type'][i] = "Surplus"


# There are zero values in both receipts and deficit for where it was FYTD value. 
# Remove these

if len(df_rec['Stage1_Source'].str.contains("Deficit").unique().tolist()) > 1:
    df_rec = df_rec[df_rec['Stage1_Source_Amount'] != 0]

if len(df_out['Stage4_Function'].str.contains("Surplus").unique().tolist()) > 1:
    df_out = df_out[df_out['Stage3_Outlay_Amount'] != 0]

df_rec.reset_index(drop=True, inplace=True)
df_out.reset_index(drop=True, inplace=True)

# Group Functions into "Other" Category so that the outlay amount of "Other" is positive
df_out.sort_values(by=['Stage3_Outlay_Amount'], ascending=True, inplace=True)
df_out.reset_index(drop=True, inplace=True)

df_out['other_group'] = "No"
#Don't include surplus in the cumulative sum; separate, manipulate, and concat back togehter
df_out_surplus = df_out[df_out['Stage3_Outlay_Type'] == "Surplus"]
df_out_nosurplus = df_out[df_out['Stage3_Outlay_Type']!="Surplus"]

df_out_nosurplus['cumsum'] = df_out['Stage3_Outlay_Amount'].cumsum()

df_out = pd.concat([df_out_nosurplus, df_out_surplus], axis=0)
df_out.reset_index(drop=True, inplace=True)



# Group all functions together into "Other" such that the cumulative sum is positive
    #Take the first function through whichever function the cum sum is positive and lump into "Other"
zero_crossing_index = np.where(np.diff(np.signbit(df_out['cumsum'])))[0] # index is 5 for where the last negative is


#I want to form an other group that is positive -- need to include first positive or "yes"

for i in range(len(df_out)):
    if i < (zero_crossing_index +2) : # +1 gets it through last negative number, we want one more to caputure the first positive cumsum value 
        df_out['other_group'][i] = "Yes"
    else:
        df_out['other_group'][i] = "No"

### Create a Stage4_Function_Other variable that includes the other category

# Rename the original and call the one with "other" "Stage4_Function"
df_out.rename(columns ={'Stage4_Function': 'Stage4_Function_Orig'}, inplace = True)
    # Make the other category
df_out['Stage4_Function_Other'] =  np.where(df_out['other_group']=="Yes", "Other", df_out['Stage4_Function_Orig'])
    # Rename the "other" version back to regular 
df_out.rename(columns ={'Stage4_Function_Other': 'Stage4_Function'}, inplace = True)

# Groupby the new stage4 function with other variable to combine all the other category's values
df_out2 = df_out.groupby(['Stage4_Function'], as_index=False)['Stage3_Outlay_Amount'].sum().reset_index(drop=True)

df_out_other = pd.merge(df_out, df_out2, how='right', on=['Stage4_Function', 'Stage3_Outlay_Amount'])
df_out_other.sort_values(by=['Stage3_Outlay_Amount'], inplace=True)
df_out_other['cumsum'] = df_out_other['Stage3_Outlay_Amount'].cumsum()

# Adding back in information we lost in the right-merge
index_other = df_out_other[(df_out_other.loc[:, 'Stage4_Function']=="Other")].index.tolist()[0]
df_out_other['Link'][index_other] = "link"
df_out_other['Stage3_Outlay_Type'][index_other] = "Outlay"
df_out_other['Stage4_Function_Orig'][index_other] = "Multiple Functions"




# Combine all "social insurance and retirement receipts" 
    # Because I grouped all 3 social insurance and retiremnt sources of receipts, we need to combine those values
df_r_grouped = df_rec.groupby(['Stage1_Source'], as_index=False)['Stage1_Source_Amount'].sum().reset_index(drop=True)
df_rec2 = pd.merge(df_r_grouped, df_rec, how='left', on=['Stage1_Source', 'Stage1_Source_Amount'])

# Adding back in information we lost in the merge
index_SIRR = df_rec2[(df_rec2.loc[:, 'Stage1_Source']=="Social Insurance and Retirement Receipts")].index.tolist()[0]
df_rec2['Link'][index_SIRR] = "link"
df_rec2['Stage2_Receipt_Type'][index_SIRR] = "Receipt"

df_rec2.sort_values(by=['Stage1_Source_Amount'], inplace=True)
df_rec2.reset_index(drop=True, inplace=True)


del df_out_other['other_group']
del df_out_other['cumsum'] # actually this ends up getting created later for Stage3 Amounts
del df_out_surplus
del df_out_nosurplus
del df_fig_cov
del df_out
del df_out2
del df_outlays
del df_receipts
del index_other
del index_SIRR


#new name = old
df_receipts = df_rec2
del df_rec2
df_outlays = df_out_other
del df_out_other

# Delete the Original Stage 4 Function column - can bring back later if this is desired
del df_outlays['Stage4_Function_Orig']

#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
#><><><><><><><><> RECEIPTS ONLY <><><><><><><><><><><><><><><><><><><><><><><>
#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
#%%
"""|--------------------------------------------------------------------|"""
"""|                   PREP THE DATA (Receipts)                         |"""
"""|--------------------------------------------------------------------|"""

### ADD IN SPACERS & FAKE SOURCE BEFORE MERGING WITH MODEL ====================
    # Create spacers so that they fall in between. Later, once their rank is 
    # in the right place, force all their values to be different
    # Spacers are for Pillar 1
    # Dummy Source is for Pillar 2

## SPACERS---------------------------------------------------------------------

# Create a copy dataframe of receipt, but (receipts values - $1) then concat with df_receipts and sort
    # Don't create a changed assignment using: df_receipts = df_r_space 
    # minus $1 helps easily spot the spacer or the real data before we change the values, and ensures it'll be in correct order

# Create a copy of receipts then rename to spacers
df_r_space = df_receipts.copy()

# Rename the sources as spacers
for i in range(len(df_r_space)):
    df_r_space['Stage1_Source_Amount'][i] = (df_r_space['Stage1_Source_Amount'][i] - 1)
    df_r_space['Stage1_Source'][i] = "Spacer " + str(i)

## DUMMY SOURCE-----------------------------------------------------------------
    # Take the smallest Spacer, which would've been deleted later, and rename to Dummy Source
    # Would've been deleted later b/c we only need spacers BETWEEN the sources, and the smallest would've fallen below last real category.
df_r_space.columns.tolist()

for i in range(len(df_r_space)):
    if df_r_space['Stage1_Source_Amount'][i]==df_r_space['Stage1_Source_Amount'].min():
        df_r_space['Stage1_Source'][i] = "Dummy_Source"

## CONCAT =====================================================================
# Concat df_receipts with the df_r_space
df_r_concat = pd.concat([df_receipts, df_r_space], axis=0)
df_r_concat.reset_index(drop=True, inplace=True)
    # Note: this currently has more spacers than we need AND a deficit-based spacer
    # get rid of this later, once rank puts the spacers where we want them

## CREATE INDICATOR TO EASILY IDENTIFY REAL/SPACER DATA =======================
    # Build in a column here indicating if it's real/fake data for later
    #(I realized i needed a way to quickly indicate the group of real/spacer data)
df_r_concat['Stage1_Row_Type']=""

for i in range(len(df_r_concat)):
    if df_r_concat['Stage1_Source'][i].startswith("Spacer"):
        df_r_concat['Stage1_Row_Type'][i] = "Spacer"
    elif df_r_concat['Stage1_Source'][i].startswith("Dummy"):
        df_r_concat['Stage1_Row_Type'][i] = "Spacer"
    else:
        df_r_concat['Stage1_Row_Type'][i] = "Real"


# Change Dummy Source's "Stage2_Receipt_Type" to "Dummy" ----------------------
for i in range(len(df_r_concat)):
    if df_r_concat['Stage1_Source'][i].startswith("Dummy"):
        df_r_concat['Stage2_Receipt_Type'][i] = "Dummy"




### MERGE SOURCE DATA WITH MODEL ============================= ================

# Now you can merge the model in (T, path, and MinMax variables)
df_receipts_merged = pd.merge(df_r_concat, model, how="inner", on="Link")
del df_receipts_merged['Link']

# Drop duplicates 
df_receipts_merged = df_receipts_merged.drop_duplicates() # none dropped


# Rename dataframes and delete
df_r = df_receipts_merged
del df_receipts_merged
del df_r_concat

#%%
"""|--------------------------------------------------------------------|"""
"""|                       CREATE RANK (N)   (Receipts)                 |"""
"""|--------------------------------------------------------------------|"""

    # "Rank" will be (1-n) ordering of the boxes
    # Position will be based on the cumsum of dollar amounts per group,i.e. height on bar graph
    # Creating a rank for pillar 1 provides an ordinal rank for pillar 2 once we remove the spacers as a separate df to manipulate
        # e.g., if rank2 goes 2, 4, 6, b/c we removed spacers, then those will still be "in order"
    # I need a (1-n) rank so that even when i change the spacers' values, they're in the right order/position



# Sort so that it's easier to inspect in the variable explorer
df_r.sort_values(by=['t','Path'], inplace=True)
df_r.reset_index(drop=True, inplace=True)


### PILLAR 1 RANK ============================================================

    # Pillar 1 rank will be based on running sum or cumulative sum or cum sum of receipt amont WITH spacers
    # IF DEFICIT
        # Force Deficit and Dummy Source to be rank 0
    # IF SURPLUS
        # Pillar 1 will just be sources + spacers    
        
    # I made each spacer $1 less than the copied value so that it'd sit below 
        # each real box when sorted, rank will match that, then we give it diff value based on aesthetics

    #Multiple times I want to manipulate the real data and force the spacers or dummy data to do something else
        # so I separate, manipulate, concat back together. For example, I dont want to straight rank and then 
        #shift every source down X ranks and then force deificit to be zero. i split them, rank the real data, 
        # force deficit to be zero, then concat them back together and i have what i want.

# Creating Rank: ---------------------------------------------------------------
    #separate, manipulate, concat back together so give all 1-n rank and force def=0.
df_r['pillar1_rank_n'] = np.nan
    # Create a df of just deficit (can leave dummy source b/c i made it smallest)
df_r_def = df_r.loc[df_r['Stage2_Receipt_Type'] == "Deficit"] 
    # Create a df of all but deficit
df_r_no_def = df_r.loc[df_r['Stage2_Receipt_Type'] != "Deficit"]

## REAL DATA 
# Add in rank (n-1) to the non-deficit df (small rank, small dollar amount)
df_r_no_def['pillar1_rank_n'] = df_r_no_def.groupby(['t','MinMax'])['Stage1_Source_Amount'].rank(ascending=True) #n-1; low values = low position


## DUMMY SOURCE DATA 
#Dummy is current ranked lowest, 1, but we want it 0, so that it sits below deficit
df_r_no_def.reset_index(drop=True, inplace=True)
for i in range(len(df_r_no_def)):
    if df_r_no_def['Stage1_Source'][i].startswith("Dummy"):
        df_r_no_def['pillar1_rank_n'][i] = 0

## DEFICIT DATA 
# Force deficit's rank==1 so it sits on the bottom of all the sources in pillar 2
df_r_def['pillar1_rank_n'] = 1
        

# Concat them back together to form df_r whole again 
df_r_new = pd.concat([df_r_no_def, df_r_def], axis=0) #wanted a unique name to double check it worked like i thought it would

df_r_new.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r_new.reset_index(drop=True, inplace=True)

df_r = df_r_new
del df_r_new
del df_r_no_def
del df_r_def



# Rename the spacer groups to match their rank so it's easier to look at--------

for i in range(len(df_r)):
    if df_r['Stage1_Source'][i].startswith("Spacer"):
        df_r['Stage1_Source'][i] = "Spacer " + str(int(df_r['pillar1_rank_n'][i]))
    else:
        df_r['Stage1_Source'][i] = df_r['Stage1_Source'][i]

df_r.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)


# Remove Spacer 1 if we're in a deficit month - unncessary 
    # Spacer 1, after renaming, should always be the one that was formerly paired with deficit
    # Rank is unchanged, since it was duplicating deficit's rank of 1.
df_r = df_r.loc[df_r['Stage1_Source'] != "Spacer 1"]


#%%
"""|--------------------------------------------------------------------|"""
"""|                  CREATE POSITION ($cumsum) (Receipts)              |"""
"""|--------------------------------------------------------------------|"""

    # The cumulative sum values will help us create the values for where the top
    # and bottom curves should hit for each source/function.

    # Pillar 1's cumulative sum will involve spacers, but not deficit or dummy source
    # Pillar 2's cumulative sum will NOT involve spacers, but WILL have deficit and dummy source

### PILLAR 2 CUMULATIVE SUMS (Receipts)  ========================================


# Don't include spacer values--> separate, manipulate, concat back together------
    #Create new column before splitting up
df_r['pillar2_cumsum'] = np.nan

    # df of just spacers
df_r_spacers = df_r.loc[df_r['Stage1_Row_Type']=="Spacer"] # this includes the dummy data, which we DO want in pillar 2
    # df without the spacers
df_r_nospace = df_r.loc[df_r['Stage1_Row_Type']=="Real"] #this is missing dummy data, so it's not quite what we need just yet
    # dummy data
df_r_dum = df_r.loc[df_r['Stage1_Source']=="Dummy_Source"]


# Change Dummy Source's value -------------------------------------------------
    # Note that sum out outlays and sum of rec+def don't exactly like up due to rounding
sum_outlays = df_outlays['Stage3_Outlay_Amount'].sum() #verified against excel - check
#dummy_amt = int(0.4*sum_outlays) # setting the dummy amount relative to oulays
dummy_amt = 100000000000 # hard coding the dummy amount for now --> could change this later
df_r_dum['Stage1_Source_Amount'] = dummy_amt


# Concat the nospace df with dummy df to form Pillar 2 set-------------------------
df_r_nospace_anddum = pd.concat([df_r_nospace, df_r_dum], axis=0)

# Ensure before we do the running sum, they're ordered by rank (i.e. how we want them stacked in Pillar 1)
df_r_nospace_anddum.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r_nospace_anddum.reset_index(drop=True, inplace=True)

# Cumulative sum  - Pillar 2---------------------------------------------------------
df_r_nospace_anddum['pillar2_cumsum'] = df_r_nospace_anddum.groupby(['t', 'MinMax'])['Stage1_Source_Amount'].cumsum()

# Before concating, remove Dummy data from the Spacers DF to avoid duplication
df_r_spacers_nodum = df_r_spacers.loc[df_r_spacers['Stage1_Source']!="Dummy_Source"]

# Concat them back together (spacers will have NaN cumsum's in pillar 2)
df_r = pd.concat([df_r_nospace_anddum, df_r_spacers_nodum], axis=0) # this will leave spacer cumsum values as NaN

df_r.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)


del df_r_nospace
del df_r_spacers
del df_r_nospace_anddum
del df_r_spacers_nodum
del df_r_dum

#%%
### PILLAR 1 CUMULATIVE SUMS (Sources with spacers) ===========================
    # Recall: we want the cumulative sum to start with smallest source (estate) and go up from there
    # We don't want to include the deficit (if applicable) or the dummy source in pillar 1

# SET ALL SPACE VALUES TO AN EQUAL VALUE --------------------------------------
    # Sum of spaces needs to exceed (deficit+dummy) so the top parts go up

# How much is the deficit? (If applicable)
if len(df_r['Stage2_Receipt_Type'].str.contains("Deficit").unique().tolist()) > 1: # if there is a deficit
    index_def = df_receipts[(df_receipts.loc[:, 'Stage2_Receipt_Type']=="Deficit")].index.tolist()[0]
    deficit = df_receipts['Stage1_Source_Amount'][index_def]

# Dummy Amount was set above
dummy_amt

# How many spacer groups do we need for receipts?
# Groups in df_receipts original data - deficit - 1 (to go in between real categories only)

if len(df_r['Stage2_Receipt_Type'].str.contains("Deficit").unique().tolist()) > 1: # if there's a deficit
    num_receipt_spacers = len(df_receipts) - 2
else:
     num_receipt_spacers = len(df_receipts) - 1


# HARD CODING THE SPACERS FOR NOW ----------------------------------------------
    # CAN BRING BACK CODE FOR MORE FLEXIBLE DECISIONS - SEE "GRAVEYARD" AT THE END

size_rec_spacer = 40000000000 # 40B



# Force the value of each spacer or leave Real data's amount as is
for i in range(len(df_r)):
    if df_r['Stage1_Source'][i].startswith("Spacer"):
        df_r['Stage1_Source_Amount'][i] = size_rec_spacer

###  Pillar 1 -----------------------------------------------------------------

# Create the Pillar 1 cumsum variable before separating
df_r['pillar1_cumsum'] = np.nan

    # df of deficit or dummy - NOT part of pillar 1
df_r_def = df_r.loc[df_r['Stage1_Source']=="Deficit"]
df_r_dum = df_r.loc[df_r['Stage1_Source']=="Dummy_Source"]

    # df without deficit or dummy - what we want to take the cumulative sum for pillar 1
df_r_nodef_nodum = df_r.loc[df_r['Stage1_Source']!="Dummy_Source"] # kept everything but dummy
df_r_nodef_nodum = df_r_nodef_nodum.loc[df_r_nodef_nodum['Stage1_Source']!="Deficit"] #kept everything but deficit

#Ensure before we do the running sum, they're ordered by rank (i.e. how we want them stacked in Pillar 1)
df_r_nodef_nodum.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r_nodef_nodum.reset_index(drop=True, inplace=True)

# Cumulative sum  - Pillar 1
df_r_nodef_nodum['pillar1_cumsum'] = df_r_nodef_nodum.groupby(['t', 'MinMax'])['Stage1_Source_Amount'].cumsum()

# Concat dummy and deficit dataframes
df_r_defdum = pd.concat([df_r_def, df_r_dum], axis=0) 

# Make the cumulative sum zero for both deficit or dummy in Pillar 1 
df_r_defdum['pillar1_cumsum'] = 0

# Concat them back together with the real data
df_r = pd.concat([df_r_nodef_nodum, df_r_defdum], axis=0) # this will leave spacer cumsum values as NaN

df_r.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)

del df_r_defdum
del df_r_nodef_nodum
del df_r_def
del df_r_dum


#%%
"""|--------------------------------------------------------------------|"""
"""|  CREATE TOP/BOTTOM POSITIONS FOR CURVES TO START/STOP (Receipts)   |"""
"""|--------------------------------------------------------------------|"""

### TOP AND BUTTOM CURVES ======================================================

# REAL aka SOURCES BOXES
    # In pillar 1, Top Line should hit the top of the source box
    # In pillar 1, Bottom Line should hit the bottom of the source box, i.e. the top of the spacer below it

    # In pillar 2, Top Line should hit the top of the same source box
    # In pillar 2, Bottom Line should hit the bottom of that same source box, i.e. the top of the source below it

# FAKE aka SPACERS
    # In pillar 1, Top Line should hit the top of the spacer box
    # In pillar 1, Bottom Line should hit the bottom of the spacer box, i.e. the top of the source below it

    # In pillar 2, Top Line should hit the bottom of the box of the source that sits above it in pillar 1
    # In pillar 2, Bottom Line should hit that same point 
    
df_r['Stage1_Top']=np.nan
df_r['Stage1_Bot']=np.nan

df_r['Stage2_Top']=np.nan
df_r['Stage2_Bot']=np.nan







### PILLAR 1 =================================================================
    # Starting with Pillar 1 because both real and spacers have same logic:
    # In pillar 1, Top Line should hit the top of the source box
    # In pillar 1, Bottom Line should hit the bottom of the source box, i.e. the top of the spacer below it


# TOP (real and spacer)
    # The TOP line should come to the value = cumulative sum 
df_r['Stage1_Top']=df_r['pillar1_cumsum']


## BOTTOM (real and spacer)

# The bottom line should come to the top of the one below it
    #Ensure things are in the right order if you're going to use (index-1) to find "below" it in the df
df_r.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)

for i in range(1, len(df_r)):
    df_r['Stage1_Bot'][i]=df_r['Stage1_Top'][i-1] #has to be i-1 if you sort by t, minxmax, rank. if you forget to sort with minmax, you'd do i-2.

# Set the smallest source's bottom to zero, since it'll be pulling in non-sensical numbers based on the loop above
    # This should be rank 0, since we made dummy the bottom somewhere above.
    
smallest_source_rank = df_r['pillar1_rank_n'].min() 
for i in range(len(df_r)):
    if df_r['pillar1_rank_n'][i]==smallest_source_rank:
        df_r['Stage1_Bot'][i] = 0



### PILLAR 2 =================================================================

#Recall:
    # REAL aka SOURCES BOXES
        # In pillar 2, Top Line should hit the top of the same source box
        # In pillar 2, Bottom Line should hit the bottom of that same source box, i.e. the top of the source below it
    # SPACERS
        # In pillar 2, Top Line should hit the bottom of the box of the source that sits above it in pillar 1
        # In pillar 2, Bottom Line should hit that same point 
        

# REAL DATA - TOP -------------------------------------------------------------
    # Make sure the df is sorted 
df_r.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)
    #The top line should come to the value = cumulative sum 
df_r['Stage2_Top']=df_r['pillar2_cumsum']


# REAL DATA - BOTTOM -------------------------------------------------------------


# The bottom line should come to the top of the one below it
df_r.sort_values(by=['t', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)
for i in range(4, len(df_r)):
    df_r['Stage2_Bot'][i]=df_r['Stage2_Top'][i-4] # i-4 to skip over the spacers if sorted as (t, pillar)

# The skipping over spacers thing by doing i-4 works everywhere BUT when estate (or lowest source)
    #dummy source and deficit are next to each other without spacers, if deficit

# Force lowest source (Estate)'s values before starting loop
df_r.sort_values(by=['t', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)

for i in range(len(df_r)):
    if df_r['pillar1_rank_n'][i]==2:
        df_r['Stage2_Bot'][i] = df_r['Stage2_Top'][i-2]

# Spacer 3 is also thrown off by this lack of symmetry that we fixed above,
    # but I will be setting Spacers' top/bottom later and overwrite this.

# Set the first 4 rows we skipped over
df_r['Stage2_Bot'][0] = 0
df_r['Stage2_Bot'][1] = 0
df_r['Stage2_Bot'][2]=df_r['Stage2_Top'][0] #the bottom of deficit is the top of dummy source
df_r['Stage2_Bot'][3]=df_r['Stage2_Top'][0] 


# Set the lowest ranked source's bottom to zero 
    # we already made dummy source rank 0 up above, so we can use that 
    # otherwise the bottom is nonsensical, it's the top of the highest
for i in range(len(df_r)):
    if df_r['pillar1_rank_n'][i]==0:
        df_r['Stage2_Bot'][i] = 0

df_r.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)

#%%
# SPACERS - TOP AND BOTTOM--------------------------------------------
# Recall:
    # SPACERS
        # In pillar 2, Top Line should hit the bottom of the box of the source that sits above it in pillar 1
        # In pillar 2, Bottom Line should hit that same point 
        
        #Equivalent - take the source below it's top value 
        
df_r.sort_values(by=['t','MinMax', 'pillar1_rank_n'], inplace=True)
df_r.reset_index(drop=True, inplace=True)
for i in range(2, len(df_r)):
    if df_r['Stage1_Source'][i].startswith("Spacer"):
        df_r['Stage2_Bot'][i]=df_r['Stage2_Top'][i-1] #(i-2 if we didnt sort with mixmax)
        df_r['Stage2_Top'][i]=df_r['Stage2_Top'][i-1]



# Force deficit's positions to be zero for both pillars, top and bottom 
# Force Dummy Source's positions to be zero for both pillars, top and bottom 
for i in range(len(df_r)):
    if df_r['Stage1_Source'][i]=="Dummy_Source":
        df_r['Stage2_Bot'][i]=0
        df_r['Stage2_Top'][i]=0
    elif df_r['Stage1_Source'][i]=="Deficit":
        df_r['Stage2_Bot'][i]=0
        df_r['Stage2_Top'][i]=0
    else:
        df_r['Stage2_Top'][i]=df_r['Stage2_Top'][i]
        df_r['Stage2_Bot'][i]=df_r['Stage2_Bot'][i]        




#%%
"""|--------------------------------------------------------------------|"""
"""|                       CREATE SIGMOID CURVES (Receipts)             |"""
"""|--------------------------------------------------------------------|"""

# SIGMOID ------------------------------------------------------------------
df_r['sigmoid'] = np.nan

for i in range(len(df_r)):    
    df_r['sigmoid'][i] = 1/(1+np.exp(1)**-(df_r['t'][i]))

# TOP CURVE ------------------------------------------------------------------
df_r.columns.tolist()
df_r['curve_top_rec'] = np.nan

for i in range(len(df_r)):    
    df_r['curve_top_rec'][i] = df_r['Stage1_Top'][i] + ((df_r['Stage2_Top'][i] - df_r['Stage1_Top'][i])*df_r['sigmoid'][i])

# BOTTOM CURVE------------------------------------------------------------------
df_r['curve_bot_rec'] = np.nan

for i in range(len(df_r)):    
    df_r['curve_bot_rec'][i] = df_r['Stage1_Bot'][i] + ((df_r['Stage2_Bot'][i] - df_r['Stage1_Bot'][i])*df_r['sigmoid'][i])






#%%
"""|--------------------------------------------------------------------|"""
"""|                      WRITE TO CSV (Receipts)                       |"""
"""|--------------------------------------------------------------------|"""

date = datetime.today().strftime("%y%m%d")
MTS_version = str(curr_mo + curr_fy)
write_receipts_path = output_dir + "/fig0_cover_" + MTS_version + "_modified_for_sankey_receipts_only_" + str(date) + ".csv"
df_r.to_csv(write_receipts_path, index=False, header=True)
print(write_receipts_path)


"""|--------------------------------------------------------------------|"""
"""|                       CLEAR ALL: RECEIPTS                           |"""
"""|--------------------------------------------------------------------|"""


# del df_r
del df_r_grouped
del df_r_space





#%%
#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
#><><><><><><>><><><><><><><><> OUTLAYS ONLY <><><><><><><><><><<><><><><><><>
#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>


"""|--------------------------------------------------------------------|"""
"""|                         PREP THE DATA (Outlays)                    |"""
"""|--------------------------------------------------------------------|"""

### ADD IN SPACERS BEFORE MERGING WITH MODEL ==================================
    # Create spacers so that they fall in between. Later, once their rank is 
    # in the right place, force all their values to be smaller and equal

## SPACERS

# Create another dataframe based on (amounts - $1) then concat with df_receipts and sort
    # Don't create a changed assignment using: df_receipts = df_r_space; create a copy

# Create a copy of outlays then rename to spacers
df_outlays_space = df_outlays.copy()


# Rename the sources as spacers
# df_outlays_space.columns.tolist()
for i in range(len(df_outlays_space)):
    df_outlays_space['Stage3_Outlay_Amount'][i] = (df_outlays_space['Stage3_Outlay_Amount'][i] - 1) #one number off to easily see which is which before changing the value later
    df_outlays_space['Stage4_Function'][i] = "Spacer " + str(i)


## DUMMY FUNCTION -----------------------------------------------------------------
    # Take the smallest Spacer, which would've been deleted later, and rename to Dummy Function
    # Would've been deleted later b/c we only need spacers BETWEEN the functions, and the smallest would've fallen below last real category.
df_outlays_space.columns.tolist()

for i in range(len(df_outlays_space)):
    if df_outlays_space['Stage3_Outlay_Amount'][i]==df_outlays_space['Stage3_Outlay_Amount'].min():
        df_outlays_space['Stage4_Function'][i] = "Dummy_Function"


## CONCAT =====================================================================
# Concat df_outlays with the spacers dataframe and reset index
df_out_concat = pd.concat([df_outlays, df_outlays_space], axis=0)
df_out_concat.reset_index(drop=True, inplace=True)
    # Note: this might have more spacers than we need 
    # get rid of this later, once rank is properly set 

### CREATE INDICATOR TO EASILY IDENTIFY REAL/SPACER DATA ================
    # Build in a column here indicating if it's real/fake data for later, when i realized i needed
    # a way to quickly indicate the group of real data and the group of spacer data
    
df_out_concat['Stage4_Row_Type']=""

for i in range(len(df_out_concat)):
    if df_out_concat['Stage4_Function'][i].startswith("Spacer"):
        df_out_concat['Stage4_Row_Type'][i] = "Spacer"
    elif df_out_concat['Stage4_Function'][i].startswith("Dummy"):
        df_out_concat['Stage4_Row_Type'][i] = "Spacer"
    else:
        df_out_concat['Stage4_Row_Type'][i] = "Real"


# Change Dummy Function's "Stage3_Outlay_Type" to "Dummy" ----------------------
for i in range(len(df_out_concat)):
    if df_out_concat['Stage4_Function'][i].startswith("Dummy"):
        df_out_concat['Stage3_Outlay_Type'][i] = "Dummy"
        
        

### MERGE WITH THE "MODEL" =====================================================

    # Now you can merge the model in (T, path, and MinMax variables)
df_out_merged = pd.merge(df_out_concat, model, how="inner", on="Link")
del df_out_merged['Link'] #this was just to get the inner merge we wanted. 

# Drop duplicates 
df_out_merged = df_out_merged.drop_duplicates() # none dropped


# Rename dataframes and delete
df_out = df_out_merged
del df_out_merged
del df_out_concat
del df_outlays_space

#%%
"""|--------------------------------------------------------------------|"""
"""|                       CREATE RANK (N) (Outlays)                    |"""
"""|--------------------------------------------------------------------|"""
    # "Rank" will be (1-n) ordering of the "boxes" in the pillars
    # Position will be based on the cumsum of dollar amounts per group ,i.e. height on bar graph
    # Creating a rank for pillar 4 provides an ordinal rank for pillar 4 once we remove the spacers as a separate df to manipulate
        # e.g., if rank3 goes 2, 4, 6, b/c we removed spacers, then those will still be "in order"
    # I need a (1-n) rank so that even when i change the spacers' values, they're in the right order/position

# Sort so that it's easier to inspect in the variable explorer
df_out.sort_values(by=['t','Path'], inplace=True) #same as grouping by t, minmax, but i thikn minmax is easier to look at because it's consistent so i swtiched
df_out.reset_index(drop=True, inplace=True)


### PILLAR 4 RANK ==============================================================
    #separate, manipulate, concat back together so give all 1-n rank and force dummy=0.
df_out['pillar4_rank_n'] = np.nan

    # Create a df of just dummy
df_out_dum = df_out.loc[df_out['Stage4_Function'] == "Dummy_Function"]

    # Create a df of just surplus
df_out_surp = df_out.loc[df_out['Stage3_Outlay_Type'] == "Surplus"]
    # Delete the duplicate surplus spacer
df_out_surp = df_out_surp.loc[df_out_surp['Stage4_Row_Type'] == "Real"]

    # Create a df of all but dummy  (real + spacers)
df_out_nodum = df_out.loc[df_out['Stage4_Function'] != "Dummy_Function"]
df_out_nodum = df_out_nodum.loc[df_out_nodum['Stage3_Outlay_Type'] != "Surplus"]


# Force Dummy's Rank for Pillar 4 to be zero
df_out_dum['pillar4_rank_n'] = 0

# Create a rank for Pillar 4 (real + spacers)
df_out_nodum.sort_values(by=['t','MinMax', 'Stage3_Outlay_Amount'], inplace=True)
df_out_nodum.reset_index(drop=True, inplace=True)
df_out_nodum['pillar4_rank_n'] = df_out_nodum.groupby(['t','MinMax'])['Stage3_Outlay_Amount'].rank(ascending=True) #n-1; low values = low position = closest to x-axis


# Force Surplus's Rank for Pillar 4 to be the highest
df_out_surp['pillar4_rank_n'] = (df_out_nodum['pillar4_rank_n'].max() + 1)


# Concat them back together
df_out = pd.concat([df_out_dum, df_out_surp], axis=0) #combine dummy and surplus
df_out = pd.concat([df_out, df_out_nodum], axis=0) # add in rest of data 

df_out.sort_values(by=['t','MinMax', 'pillar4_rank_n'], inplace=True)
df_out.reset_index(drop=True, inplace=True)


# Rename the spacer groups to match their rank so it's easier to look at
for i in range(1, len(df_out)):
    if df_out['Stage4_Function'][i].startswith("Spacer"):
        df_out['Stage4_Function'][i] = "Spacer " + str(int(df_out['pillar4_rank_n'][i])) 
    else:
        df_out['Stage4_Function'][i] = df_out['Stage4_Function'][i]

df_out.sort_values(by=['t','MinMax', 'pillar4_rank_n'], inplace=True)
df_out.reset_index(drop=True, inplace=True)


# Clean up -------------------------------
del df_out_dum
del df_out_nodum
del df_out_surp




#%%
"""|--------------------------------------------------------------------|"""
"""|                 CREATE POSITION ($cumsum) (Outlays)                |"""
"""|--------------------------------------------------------------------|"""
    # Create cumsum dollar-based position -- this is where the curve will "hit"
    # on pillars 3 and 4. Use dollar amounts for these to match a stacked bar chart

### CHANGE DUMMY FUNCTION VALUE ==================================================
    # Dummy Function's value was currently $1 less than smallest function--too small for space purpose
    # A dummy amount of X % of outlays was set above

# A better place to make this change would be below, when we are already separating out 
# dummy, spacers, and real data



### PILLAR 3 POSITIONS/CUMSUM (OUTLAYS) ========================================
    # To create Pillar 3's positions, we don't need the spacer data
    # Don't include spacer values--> separate, manipulate, concat back together
    # For Pillar 3's cumsum, we want real data + dummy + surplus if there is one

# Just the spacers ----------------------------------------------------------
df_out_spacers = df_out.loc[df_out['Stage4_Row_Type']=="Spacer"] #all data labeled spacers, including dummy
df_out_spacers_nodum = df_out_spacers.loc[df_out_spacers['Stage3_Outlay_Type'] != "Dummy"] #removing dummy data
    # Sort 
df_out_spacers_nodum.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)
df_out_spacers_nodum.reset_index(drop=True, inplace=True)

# Real data plus dummy------------------------------------------------------

    #just the dummy
df_dum = df_out.loc[df_out['Stage3_Outlay_Type']=="Dummy"] 

    # Change the dummy amount here, as mentioned above
df_dum['Stage3_Outlay_Amount'] = dummy_amt


# Real data plus dummy, cont'd
df_out_nospace = df_out.loc[df_out['Stage4_Row_Type']=="Real"] #just the real (includes surplus)
df_out_nospace_anddum = pd.concat([df_out_nospace,df_dum], axis=0) #concat the two for pillar 3 cumsum
    # Sort 
df_out_nospace_anddum.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)
df_out_nospace_anddum.reset_index(drop=True, inplace=True)



# Cumulative sum----------------------------------------------------------------
df_out_nospace_anddum['pillar3_cumsum'] = df_out_nospace_anddum.groupby(['t', 'MinMax'])['Stage3_Outlay_Amount'].cumsum()
df_out = pd.concat([df_out_nospace_anddum, df_out_spacers_nodum], axis=0) # this will leave spacer cumsum values as NaN

df_out.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)
df_out.reset_index(drop=True, inplace=True)
# these numbers match with drawn out pillar 3 - check 


# Clean up ------------------------------------
del df_out_spacers
del df_out_nospace
del df_out_spacers_nodum
del df_dum
del df_out_nospace_anddum



### PILLAR 4 POSITIONS/CUMSUM (FUNCTIONS) =====================================
    # To create Pillar 4's positions, we DO need the spacers, but NOT the dummy function, and surplus if applicable 
    # Currently, spacers are equal to $1 less than the box above it. 
    # Let's make each spacer equal to some smaller, constant value (come back and play with this for aesthetics)
    
    
# SPACER SIZE FOR FUNCTIONS - EQUAL SPACING DIDN'T WORK FOR THE SMALLER VALUES
#This was done above in receipts
size_rec_spacer

# SET THE TOP, MIDDLE, AND BOTTON THIRD DIFFERENTLY -- come back and play with these values 
# AGAIN, THIS IS HARD-CODED AND CAN BE FORMULA BASED LATER BASED ON AESTHETIC DECISIONS 
"""
EXAMPLE:
Bottom 4 - 40B spacer (160B)
Mid 4 - 25B spacer (100B) 
Top 4 - 10B spacer (40B) 
"""

# x is the interval size if i split into three groups
x = int(df_out['pillar4_rank_n'].max()/3)

# Recall that low rank = low value | high rank = high value
# We want the smaller ranked spacers to have the larger value 
bot_spacer_size = 40000000000 # smallest rank is on the bottom and needs largest spacer
mid_spacer_size = 25000000000
top_spacer_size = 10000000000 # highest rank

for i in range(len(df_out)):
    if ((df_out['pillar4_rank_n'][i] < x) & (df_out['Stage4_Function'][i].startswith("Spacer"))):
        df_out['Stage3_Outlay_Amount'][i] = bot_spacer_size
    elif ((df_out['pillar4_rank_n'][i] >= x) & (df_out['pillar4_rank_n'][i] < 2*x) & (df_out['Stage4_Function'][i].startswith("Spacer"))):
        df_out['Stage3_Outlay_Amount'][i] = mid_spacer_size
    elif ((df_out['pillar4_rank_n'][i] >= 2*x) & (df_out['Stage4_Function'][i].startswith("Spacer"))):
        df_out['Stage3_Outlay_Amount'][i] = top_spacer_size




# CREATE PILLAR 4 CUMULATIVE SUM  -------------------------------------------
    # Separate, manipulate, and concat them back together : cumsum for Pillar4 needs all but dummy

df_out['pillar4_cumsum'] = np.nan

# Dummy only
df_out_dum = df_out.loc[df_out['Stage3_Outlay_Type']=="Dummy"] 
df_out_dum['pillar4_cumsum'] = 0

# All but dummy
df_out_nodum = df_out.loc[df_out['Stage3_Outlay_Type'] != "Dummy"] 


df_out_nodum.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)
df_out_nodum.reset_index(drop=True, inplace=True)
df_out_nodum['pillar4_cumsum'] = df_out_nodum.groupby(['t', 'MinMax'])['Stage3_Outlay_Amount'].cumsum()


df_out = pd.concat([df_out_dum, df_out_nodum], axis=0) # this will leave dummy's pillar 4 cumsum as NaN
df_out.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)



del df_out_dum
del df_out_nodum




#%%
"""|--------------------------------------------------------------------|"""
"""|  CREATE TOP/BOTTOM POSITIONS FOR CURVES TO START/STOP (Outlays)    |"""
"""|--------------------------------------------------------------------|"""

### TOP AND BUTTOM CURVES ======================================================

# REAL aka FUNCTION BOXES
    # In pillar 3, Top Line should hit the top of the function box
    # In pillar 3, Bottom Line should hit the bottom of the function box

    # In pillar 4, Top Line should hit the top of the same function box
    # In pillar 4, Bottom Line should hit the bottom of the box, i.e. the top of the spacer below it

# FAKE aka SPACERS
    # In pillar 4, Top Line should hit the top of the spacer box
    # In pillar 4, Bottom Line should hit the bottom of the spacer box, i.e. the top of the functio below it

    # In pillar 3, Top Line should hit the bottom of the box of the function that sits above it in pillar 4
    # In pillar 3, Bottom Line should hit that same point 

df_out['Stage4_Top']=np.nan
df_out['Stage4_Bot']=np.nan

df_out['Stage3_Top']=np.nan
df_out['Stage3_Bot']=np.nan



### PILLAR 4 POSITIONS FOR TOP/BOTTOM CURVES ===================================
    # Starting with Pillar 4 b/c top/bottom will follow the same logic for both real and spocer data

## TOP (real and spacer) ------------------------------------------------------
# The TOP line should come to the value = cumulative sum 

df_out['Stage4_Top']=df_out['pillar4_cumsum']

## BOTTOM (real and spacer)---------------------------------------------------

# The BOTTOM line should come to bottom of the box, i.e. the top of the one below it
    #Ensure things are in the right order if you're going to use (index-1) to find "below" it in the df
df_out.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)
df_out.reset_index(drop=True, inplace=True)

for i in range(1, len(df_out)):
    df_out['Stage4_Bot'][i]=df_out['Stage4_Top'][i-1] #i-1 if we sort as above
    # Dummy is Index=0, so we'll deal with dummy top/bottom's later

# Set Dummy Top/Box to Zero ---------------------------------------------------
# Dummy
df_out_dum = df_out.loc[df_out['Stage3_Outlay_Type']=="Dummy"] 
# All but dummy
df_out_nodum = df_out.loc[df_out['Stage3_Outlay_Type'] != "Dummy"] 

# Set Dummy's Stage 4 top and bottom to zero 
df_out_dum['Stage4_Top'] = 0
df_out_dum['Stage4_Bot'] = 0

df_out = pd.concat([df_out_dum, df_out_nodum], axis=0) # this will leave spacer cumsum values as NaN
df_out.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)

del df_out_dum
del df_out_nodum


#Set the lowest ranked function's bottom to zero 
    # in PILLAR 4 ('Other' in this case) ----------------------------------------

# Dummy = rank 0
# Smallest real function  = rank 1
 
for i in range(len(df_out)):
    if df_out['pillar4_rank_n'][i]==1: 
        df_out['Stage4_Bot'][i] = 0




### PILLAR 3 POSITIONS FOR TOP/BOTTOM CURVES ====================================

#Recall:
    # REAL aka FUNCTION BOXES
        # In pillar 3, Top Line should hit the top of the function box
        # In pillar 3, Bottom Line should hit the bottom of the function box
    # FAKE aka SPACERS
        # In pillar 3, Top AND bottom lines should hit same point (bottom of function above it in pillar 4
           # or the top of the function below it in pillar 4)
        # EX: Spacer 24's top and bottom lines should both hit the bottom of SS/top of Medicare in Pillar 3
        #(Using MTS 05-17))
    # DUMMY
        # Should be zero for both top and bottom 

## REAL -------------------------------------------------------------

# Make sure the df is sorted 
df_out.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)
df_out.reset_index(drop=True, inplace=True)

# TOP ---------------------------------------------------------------------------
#The top line should come to the value = cumulative sum 
df_out['Stage3_Top']=df_out['pillar3_cumsum']


## BOTTOM ----------------------------------------------------------------------
# The bottom line should come to the top of the one below it
for i in range(2, len(df_out)):
    df_out['Stage3_Bot'][i]=df_out['Stage3_Top'][i-2] # i-2 to skip over spacers

# Skipped over ones: 
    #This will get fixed when we address dummy and lowest ranked below


# Fix the lowest ranked function's bottom -----------------------------
    # The bottom of the lowest ranked function should come to the top of the dummy function
    #but the loop had it pulling the top of the highest (SS in this case)

df_out.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)
df_out.reset_index(drop=True, inplace=True)

for i in range(len(df_out)):
    if df_out['pillar4_rank_n'][i]==1: 
        df_out['Stage3_Bot'][i] = df_out['Stage3_Top'][i-1] 





#%%
# SPACERS - TOP AND BOTTOM-----------------------------------------------------
    #Recall in Pillar 3, Spacers' tops and bottoms are the same point
    # Spacer's top/bottom should both hit the function below it in pillar 4's top in pillar 3
    # EX: Spacer 24 top and bottom lines should both hit the top of Medicare in Pillar 3 (May 2017 data)
df_out.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)
df_out.reset_index(drop=True, inplace=True)

for i in range(1, len(df_out)):
    if df_out['Stage4_Function'][i].startswith("Spacer"):
        df_out['Stage3_Bot'][i]=df_out['Stage3_Top'][i-1]
        df_out['Stage3_Top'][i]=df_out['Stage3_Top'][i-1]
        

# Fix Surplus -----------------------------------------------------
for i in range(len(df_out)):
    if df_out['Stage4_Function'][i]=="Surplus":
        df_out['Stage3_Bot'][i]=df_out['Stage3_Top'][i-1]
        df_out['Stage3_Top'][i]=df_out['pillar3_cumsum'][i]
        df_out['Stage4_Top'][i]=df_out['pillar4_cumsum'][i]


# Set Dummy Top/Box to Zero for Pillar 3 -------------------------------------
    # Do this at the end so all the other top/bottoms hit the right spot
# Dummy
df_out_dum = df_out.loc[df_out['Stage3_Outlay_Type']=="Dummy"] 
# All but dummy
df_out_nodum = df_out.loc[df_out['Stage3_Outlay_Type'] != "Dummy"] 

# Set Dummy's Stage 3 top and bottom to zero 
df_out_dum['Stage3_Top'] = 0
df_out_dum['Stage3_Bot'] = 0

df_out = pd.concat([df_out_dum, df_out_nodum], axis=0) 
df_out.sort_values(by=['t', 'MinMax', 'pillar4_rank_n'], inplace=True)

del df_out_dum
del df_out_nodum

# Force surplus's positions to be zero for both pillars, top and bottom 
    # so that the polygon doesn't fill in for surplus (analogous to deficit)
    # otherwise, we'd need 3 separate data sources for pillar 3 (to shade surplus), the sankey (white), and pillar 4 (white)

for i in range(len(df_out)):
    if df_out['Stage4_Function'][i]=="Surplus":
        df_out['Stage3_Bot'][i]=0
        df_out['Stage3_Top'][i]=0
        df_out['Stage4_Bot'][i]=0
        df_out['Stage4_Top'][i]=0


#%%

"""|--------------------------------------------------------------------|"""
"""|                  CREATE SIGMOID CURVES (Outlays)                   |"""
"""|--------------------------------------------------------------------|"""

# SIGMOID ------------------------------------------------------------------
df_out['sigmoid'] = np.nan

for i in range(len(df_out)):    
    df_out['sigmoid'][i] = 1/(1+np.exp(1)**-(df_out['t'][i]))

# TOP CURVE ------------------------------------------------------------------
#df_out.columns.tolist()
df_out['curve_top_out'] = np.nan

for i in range(len(df_out)):    
    df_out['curve_top_out'][i] = df_out['Stage3_Top'][i] + ((df_out['Stage4_Top'][i] - df_out['Stage3_Top'][i])*df_out['sigmoid'][i])

# BOTTOM CURVE------------------------------------------------------------------
df_out['curve_bot_out'] = np.nan

for i in range(len(df_out)):    
    df_out['curve_bot_out'][i] = df_out['Stage3_Bot'][i] + ((df_out['Stage4_Bot'][i] - df_out['Stage3_Bot'][i])*df_out['sigmoid'][i])


#%%

"""|--------------------------------------------------------------------|"""
"""|                      WRITE TO CSV (Outlays)                        |"""
"""|--------------------------------------------------------------------|"""

date = datetime.today().strftime("%y%m%d")
write_outlays_path = output_dir + "/fig0_cover_" + MTS_version + "_modified_for_sankey_outlays_only_" + str(date) + ".csv"
df_out.to_csv(write_outlays_path, index=False, header=True)
print(write_outlays_path)


###############################################################################
###############################################################################
################################# END FILE ####################################
###############################################################################
###############################################################################

endtime = datetime.today()
end = time.time()
total_run_time_sec = (end-start) #yields seconds
total_run_time_min = int(total_run_time_sec/60)
total_run_time_hr = total_run_time_min/60 


print("***************      You started at " + str(starttime) + "      *********************")
print("***************      THE END!       " + str(endtime) + "      *********************")
print("")
print("***************      The total run time was " + str(total_run_time_min) + " minutes.        *************")
print("***************      The total run time was " + str(total_run_time_hr) + " hours.        *************")


if len(df_r['Stage2_Receipt_Type'].str.contains("Deficit").unique().tolist()) > 1:
    print("This month, there was a deficit. You should use the Tableau workbook for deficits.")
else: 
    print("This month, there was a surplus. You should use the Tableau workbook for surpluses.")