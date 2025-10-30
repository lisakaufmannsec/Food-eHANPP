# -*- coding: utf-8 -*-



"""
Title: Main calculation and figure plots
Author: Lisa Kaufmann
Affiliation: BOKU University, Institute of Social Ecology, Department of Economics and Social Sciences, Schottenfeldgasse 29, 1070 Vienna, Austria
Contact: lisa.kaufmann@boku.ac.at
Date: October 30, 2025
Version: 1.0.0
License: Creative Commons Attribution 4.0 International (CC BY 4.0) license
Repository: https://github.com/lisakaufmannsec/Food-eHANPP
Manuscript: "Income level and urbanization shape food-related pressures on ecosystems"
Description: this code is the main calculation of urban and rural Food-eHANPP and the code for the figures 3 and 4 of the mansucript.
"""


#Script loads product level eHANPP dataset for 190 countries from 1990 to 2020 by use
#and calculates Food-eHANPP intensity (t dm/kcal) from food supply
#as well as urban/rural differences
#plus: Figure (3,4) plotting and creating of figure data xlsx for SI
# Please note that Figure 1 has been created with a Sankey program and data is available in the Supplementary Data

#--> either set the working directory on the folder with the two data downloads 
#    or define a path in Line 40

###############################################################################
##                    Load and organise eHANPP dataset                       ##
###############################################################################


import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import dask.dataframe as dd

link = r'https://raw.githubusercontent.com/lisakaufmannsec/Food-eHANPP/main'

path = r'' #Place of the downloaded csv's (product-level HANPP dataset from Zenodo and extracted zip food suppyl from github)
 

#1 Load eHANPP data and organize it as needed
ddf = dd.read_csv('/embodied_HANPP_all_uses_incl_ap_all_cl_gl_resid_infra_by_animal_products_zenodo.csv', 
                  dtype={'Destination_code_FAO': 'float64','primary_product_Code': 'object', 'Origin_code_FAO': 'float64'})

df_all = ddf.compute()# Convert the Dask DataFrame to a pandas DataFrame

# Undo specification that has been done for Zenodo:
df_all['primary_product'] = df_all['primary_product'].replace('Agri. Infrastructure', 'Infrastructure')

df_all['primary_product_Code'] = df_all['primary_product_Code'].replace('Agri.Infra', 'Infrastructure')


#Select only Food Items = drop Unknown and other uses
df_food = df_all.loc[~df_all['Final_use'].isin(['Unknown', 'Other uses'])]

#reduce dataframe volume by summing up by Destination = Country of consumption:
df_food = df_food.groupby(['Destination','Destination_code_FAO','Year','Final_use',
                           'primary_product','primary_product_Code']).sum(
                               'HANPP_embodied_in_trade').reset_index().drop(
                                   'Origin_code_FAO', axis=1)

###############################################################################
#Add regions and dismiss countries that are not considered --> Food_1
#reduces n countries from 217 to 191 (later removal North Korea:190)
df0_countries = pd.read_excel(link + '/look_up.xlsx', sheet_name = 'country_groups')
df_countries = df0_countries[['code_FAO',2020,'GDD_code']].rename(columns={'code_FAO': 'Destination_code_FAO', 2020: 'income_group'})

df_food_1 = df_food.merge(df_countries, how='left', on=['Destination_code_FAO'])
df_food_1 = df_food_1.dropna(subset=['income_group'])

Countries_considered = df_food_1.loc[:, ['Destination_code_FAO']].drop_duplicates()

#Reorder columns: 
df_food_1 = df_food_1[['Destination_code_FAO','Destination','GDD_code','income_group','Year',
                       'Final_use','primary_product_Code','primary_product','HANPP_embodied_in_trade']]

###############################################################################
#Add food groups. This requires a separation of infrastructure data, an addition of the food-group infra column and then back with infra data  --> Food2
#infra extra and add food group infra
df_infra = df_food_1.loc[df_food_1['primary_product_Code'] == 'Infrastructure']
df_infra = df_infra.assign(food_group='Infrastructure')

#removing infra
df_food_2 = df_food_1.loc[df_food_1['primary_product_Code'] != 'Infrastructure']
df_food_2['primary_product_Code'] = pd.to_numeric(df_food_2['primary_product_Code'])

# join food groups 
df0_food_groups = pd.read_excel(link + '/look_up.xlsx', sheet_name = 'products')
df_food_groups = df0_food_groups[['primary_product_Code','food_group']].drop_duplicates()

df_food_2 = df_food_2.merge(df_food_groups, how = 'left', on='primary_product_Code') 

#concat infra
df_food_2 = pd.concat([df_food_2,df_infra])

#check nans: 
nan_df_food_2 = df_food_2[df_food_2.isna().any(axis=1)]

###############################################################################
#add population data
df0_pop_tot = pd.read_excel(link + '/look_up.xlsx', sheet_name = 'total_population')
df_pop_tot_nat = df0_pop_tot.drop(['Unnamed: 0','Unnamed: 1','Country','world_region','GDD_code'], axis=1)
df_pop_tot_nat = df_pop_tot_nat.set_index(['code_FAO'])
df_pop_tot_nat = df_pop_tot_nat.stack().reset_index()
df_pop_tot_nat = df_pop_tot_nat.rename(columns={'code_FAO': 'Destination_code_FAO','level_1': 'Year', 0: 'pop_national'})
df_pop_tot_nat['pop_national'] = df_pop_tot_nat['pop_national'] * 1000 # Umrechnung von 1000 cap zu cap
df_pop_tot_nat = df_pop_tot_nat.drop(df_pop_tot_nat[df_pop_tot_nat['Year'] == 'GDD_superregion'].index)
df_pop_tot_nat['pop_national'] = df_pop_tot_nat['pop_national'].astype(float)

#merge with considered countries so that countries like Russia/USSR not counted twice
df_countries_pop = df_food_1[['Year','Destination_code_FAO']].drop_duplicates()
df_pop_tot_nat = df_countries_pop.merge(df_pop_tot_nat, how='left', on=['Destination_code_FAO','Year'])

#population of income groups
df_pop_tot_reg = df_pop_tot_nat.merge(df_countries, how='left', on='Destination_code_FAO')
df_pop_tot_reg = df_pop_tot_reg.groupby(['Year','income_group']).sum('pop_national').reset_index().drop('Destination_code_FAO', axis=1)
df_pop_tot_reg = df_pop_tot_reg.rename(columns={'pop_national': 'pop_regional'})

#global population
df_pop_tot_glo = df_pop_tot_nat.groupby(['Year']).sum('pop_national').drop('Destination_code_FAO', axis=1)
df_pop_tot_glo = df_pop_tot_glo.rename(columns={'pop_national': 'global population'})

#global for plot in billion
df_pop_tot_plot = df_pop_tot_glo/1000/1000/1000 #Conversion to billion

#Urban population
df0_pop_urb = pd.read_excel(link + '/look_up.xlsx', sheet_name = 'urban_population')
df_pop_urb = df0_pop_urb.drop(['SHARE','Unnamed: 1','Country','world_region','GDD_code'], axis=1)
df_pop_urb= df_pop_urb.set_index(['code_FAO'])
df_pop_urb = df_pop_urb.stack().reset_index()
df_pop_urb = df_pop_urb.rename(columns={'code_FAO': 'Destination_code_FAO','level_1': 'Year', 0: 'pop_urb_share'})
df_pop_urb['pop_urb_share'] = df_pop_urb['pop_urb_share'] / 100 # Conversion from % to decimal number

#add to total national population
df_pop_nat = df_pop_tot_nat.merge(df_pop_urb, how = 'left', on=['Destination_code_FAO','Year'] )
df_pop_nat['urban population'] = df_pop_nat['pop_national'] * df_pop_nat['pop_urb_share']
df_pop_nat['rural population'] = df_pop_nat['pop_national'] - df_pop_nat['urban population']

#create regional population including urban population
df_pop_reg = df_pop_nat.merge(df_countries, how='left', on=['Destination_code_FAO'])
df_pop_reg = df_pop_reg.groupby(['Year','income_group']).sum(['pop_national','urban population','rural population']).reset_index()
df_pop_reg = df_pop_reg.drop(['Destination_code_FAO','pop_urb_share'], axis=1)
df_pop_reg = df_pop_reg.rename(columns={'pop_national': 'pop_regional'})
df_pop_reg['pop_urb_share'] = df_pop_reg['urban population'] / df_pop_reg['pop_regional']

###############################################################################
##                      Urban and rural Food-eHANPP                          ##
###############################################################################
#load Global Dietary Database (GDD) lookup
df0_GDD_FAO = pd.read_excel(link + '/look_up.xlsx', sheet_name = 'products')
df_GDD_FAO = df0_GDD_FAO.drop(['primary_product','food_group'], axis=1).drop_duplicates()

#load GDD urban/rural data
df0_GDD_data_median = pd.read_csv(link + '/GDD_data_collection_median_random.csv', encoding='latin-1')
df_GDD_data_median = df0_GDD_data_median.rename(columns={'Country': 'GDD_code','GDD_urban':'GDD_urban_median','GDD_rural':'GDD_rural_median'})

df0_GDD_data_upper = pd.read_csv(link + '/GDD_data_collection_upperci_95_random.csv', encoding='latin-1')
df_GDD_data_upper = df0_GDD_data_upper.rename(columns={'Country': 'GDD_code','GDD_urban':'GDD_urban_upper','GDD_rural':'GDD_rural_upper'})

df0_GDD_data_lower = pd.read_csv(link + '/GDD_data_collection_lowerci_95_random.csv', encoding='latin-1')
df_GDD_data_lower = df0_GDD_data_lower.rename(columns={'Country': 'GDD_code','GDD_urban':'GDD_urban_lower','GDD_rural':'GDD_rural_lower'})

###############################################################################
#df-food2 to df-food3: add GDD data 

#extract infrastructure for merge (and add it later again)
df_food_3 = df_food_2.loc[df_food_2['primary_product_Code'] != 'Infrastructure']

#now GDD codes can be added
df_food_3 = df_food_3.merge(df_GDD_FAO, how='left', on=['primary_product_Code'])
#Reorder columns: 
df_food_3 = df_food_3[['Destination_code_FAO','Destination','income_group','GDD_code','Year','Final_use','food_group','primary_product','primary_product_Code','GDD_item_code','GDD_item','HANPP_embodied_in_trade']]
#add GDD data 
df_food_3 = df_food_3.merge(df_GDD_data_median, how='left', on=['GDD_code','Year','GDD_item_code'])
df_food_3 = df_food_3.merge(df_GDD_data_upper, how='left', on=['GDD_code','Year','GDD_item_code'])
df_food_3 = df_food_3.merge(df_GDD_data_lower, how='left', on=['GDD_code','Year','GDD_item_code'])

#correct nans:
    #1 city states: 0 rural population 
condition_Stadtsaaten = df_food_3['GDD_code'] == 'SGP'
df_food_3.loc[condition_Stadtsaaten, 'GDD_rural_median'] = df_food_3.loc[condition_Stadtsaaten, 'GDD_rural_median'].fillna(0)
df_food_3.loc[condition_Stadtsaaten, 'GDD_rural_upper'] = df_food_3.loc[condition_Stadtsaaten, 'GDD_rural_upper'].fillna(0)
df_food_3.loc[condition_Stadtsaaten, 'GDD_rural_lower'] = df_food_3.loc[condition_Stadtsaaten, 'GDD_rural_lower'].fillna(0)

    #2 Stimulants and Spices are seperated equally between urban and urual due to missing GDD values
condition_stimulants = df_food_3['food_group'] == 'Sugars and stimulants'
df_food_3.loc[condition_stimulants, 'GDD_rural_median'] = df_food_3.loc[condition_stimulants, 'GDD_rural_median'].fillna(1)
df_food_3.loc[condition_stimulants, 'GDD_rural_upper'] = df_food_3.loc[condition_stimulants, 'GDD_rural_upper'].fillna(1)
df_food_3.loc[condition_stimulants, 'GDD_rural_lower'] = df_food_3.loc[condition_stimulants, 'GDD_rural_lower'].fillna(1)
df_food_3.loc[condition_stimulants, 'GDD_urban_median'] = df_food_3.loc[condition_stimulants, 'GDD_urban_median'].fillna(1)
df_food_3.loc[condition_stimulants, 'GDD_urban_upper'] = df_food_3.loc[condition_stimulants, 'GDD_urban_upper'].fillna(1)
df_food_3.loc[condition_stimulants, 'GDD_urban_lower'] = df_food_3.loc[condition_stimulants, 'GDD_urban_lower'].fillna(1)
df_food_3.loc[condition_stimulants, 'GDD_item'] = df_food_3.loc[condition_stimulants, 'GDD_item'].fillna('XX')

    #3 no GDD for North Korea: removed
df_food_3 = df_food_3.query('GDD_code != "PRK"')    

    #4 Somalia has no urban/rural for livestock products. We apply the difference from Ethopia
nan_indices = df_food_3[(df_food_3['GDD_code'] == 'SOM') & (df_food_3['GDD_urban_median'].isna())].index
nan_indices = df_food_3[(df_food_3['GDD_code'] == 'SOM') & (df_food_3['GDD_urban_upper'].isna())].index
nan_indices = df_food_3[(df_food_3['GDD_code'] == 'SOM') & (df_food_3['GDD_urban_lower'].isna())].index

# Iterate over the nan indices and fill NaN values with corresponding values from 'ETH'
#median
for idx in nan_indices:
    year = df_food_3.loc[idx, 'Year']
    product = df_food_3.loc[idx, 'GDD_item_code']
    
    # Find the corresponding value in 'ETH'
    eth_value = df_food_3[(df_food_3['GDD_code'] == 'ETH') & (df_food_3['Year'] == year) & (df_food_3['GDD_item_code'] == product)]['GDD_urban_median'].values[0]
    
    # Replace NaN with the 'ETH' value
    df_food_3.at[idx, 'GDD_urban_median'] = eth_value
    
nan_indices = df_food_3[(df_food_3['GDD_code'] == 'SOM') & (df_food_3['GDD_rural_median'].isna())].index

  # Iterate over the nan indices and fill NaN values with corresponding values from 'ETH'
for idx in nan_indices:
    year = df_food_3.loc[idx, 'Year']
    product = df_food_3.loc[idx, 'GDD_item_code']
      
    # Find the corresponding value in 'ETH'
    eth_value = df_food_3[(df_food_3['GDD_code'] == 'ETH') & (df_food_3['Year'] == year) & (df_food_3['GDD_item_code'] == product)]['GDD_rural_median'].values[0]
     
    # Replace NaN with the 'ETH' value
    df_food_3.at[idx, 'GDD_rural_median'] = eth_value  
      
#upper
for idx in nan_indices:
    year = df_food_3.loc[idx, 'Year']
    product = df_food_3.loc[idx, 'GDD_item_code']
    
    # Find the corresponding value in 'ETH'
    eth_value = df_food_3[(df_food_3['GDD_code'] == 'ETH') & (df_food_3['Year'] == year) & (df_food_3['GDD_item_code'] == product)]['GDD_urban_upper'].values[0]
    
    # Replace NaN with the 'ETH' value
    df_food_3.at[idx, 'GDD_urban_upper'] = eth_value
    
nan_indices = df_food_3[(df_food_3['GDD_code'] == 'SOM') & (df_food_3['GDD_rural_upper'].isna())].index

  # Iterate over the nan indices and fill NaN values with corresponding values from 'ETH'
for idx in nan_indices:
    year = df_food_3.loc[idx, 'Year']
    product = df_food_3.loc[idx, 'GDD_item_code']
      
    # Find the corresponding value in 'ETH'
    eth_value = df_food_3[(df_food_3['GDD_code'] == 'ETH') & (df_food_3['Year'] == year) & (df_food_3['GDD_item_code'] == product)]['GDD_rural_upper'].values[0]
     
    # Replace NaN with the 'ETH' value
    df_food_3.at[idx, 'GDD_rural_upper'] = eth_value      
    
#lower
for idx in nan_indices:
    year = df_food_3.loc[idx, 'Year']
    product = df_food_3.loc[idx, 'GDD_item_code']
    
    # Find the corresponding value in 'ETH'
    eth_value = df_food_3[(df_food_3['GDD_code'] == 'ETH') & (df_food_3['Year'] == year) & (df_food_3['GDD_item_code'] == product)]['GDD_urban_lower'].values[0]
    
    # Replace NaN with the 'ETH' value
    df_food_3.at[idx, 'GDD_urban_lower'] = eth_value
    
nan_indices = df_food_3[(df_food_3['GDD_code'] == 'SOM') & (df_food_3['GDD_rural_lower'].isna())].index

  # Iterate over the nan indices and fill NaN values with corresponding values from 'ETH'
for idx in nan_indices:
    year = df_food_3.loc[idx, 'Year']
    product = df_food_3.loc[idx, 'GDD_item_code']
      
    # Find the corresponding value in 'ETH'
    eth_value = df_food_3[(df_food_3['GDD_code'] == 'ETH') & (df_food_3['Year'] == year) & (df_food_3['GDD_item_code'] == product)]['GDD_rural_lower'].values[0]
     
    # Replace NaN with the 'ETH' value
    df_food_3.at[idx, 'GDD_rural_lower'] = eth_value      
    

nan_df_food_3 = df_food_3[df_food_3.isna().any(axis=1)] #! should be 0 rows

#df-food 4 = df_food_3 + Food_infra:
#add infrastrcuture again
df_infra[['GDD_item_code','GDD_item','food_group']] = 'Infra'
df_infra[['GDD_urban_median','GDD_rural_median','GDD_urban_upper','GDD_rural_upper','GDD_urban_lower','GDD_rural_lower']] = 1

df_food_4 = pd.concat([df_food_3, df_infra])
nan_df_food_4 = df_food_4[df_food_4.isna().any(axis=1)] #! should be 0 rows


###############################################################################
#                         Food supply in kcal                                 #
###############################################################################
#load food supply
df0_food_supply = pd.read_csv(path + '/food_supply.csv', encoding='latin-1')
head_df0_food_supply = df0_food_supply.head(500)
df_food_supply = df0_food_supply.drop(['Unnamed: 0','food_group','GDD_superregion'], axis=1)

#kcal
df0_kcal = pd.read_excel(link + '/look_up.xlsx', sheet_name = 'factors')
df_kcal = df0_kcal[['primary_product_Code','dm_content','kcal/g']]

#df_food5 = "Master-Table"
#add population
df_food_5 = df_food_4.merge(df_pop_nat, how ='left',on=['Destination_code_FAO','Year'])

#add supply (again Infrastructure needs to cut and add)
df_infra_5 = df_food_5.loc[df_food_5['primary_product'] == 'Infrastructure']
df_food_5 = df_food_5.loc[df_food_5['primary_product'] != 'Infrastructure']
df_food_5['primary_product_Code'] = df_food_5['primary_product_Code'].astype(float)
df_food_5 = df_food_5.merge(df_food_supply, how = 'left', on= ['Destination_code_FAO','Destination',
                                                               'GDD_code','Year','primary_product','primary_product_Code'])
#add kcal
df_food_5 = df_food_5.merge(df_kcal, how = 'left', on= ['primary_product_Code'])
#add infra again
df_infra_5 = df_infra_5.assign(tonnes_traded_dm='nan')
df_food_5 = pd.concat([df_food_5,df_infra_5])

###############################################################################
#                              Calculations                                   #
###############################################################################
#Calculations in df_food6
df_food_6 = df_food_5.copy()
#Supply in kcal
df_food_6['tonnes_traded_fw'] = df_food_6['tonnes_traded_dm'] / df_food_6['dm_content']
df_food_6['kcal_traded'] = df_food_6['tonnes_traded_fw'] * 1000 * 1000 * df_food_6['kcal/g']
df_food_6['kcal/cap/day'] = (df_food_6['kcal_traded'] / df_food_6['pop_national']) /365
df_food_6 = df_food_6.drop(['dm_content','tonnes_traded_fw','kcal/g'], axis=1)

#urban/rural
#median
df_food_6['FeH_urban_median'] = ((df_food_6['GDD_urban_median'] * df_food_6['urban population']) / (df_food_6['GDD_urban_median'] * df_food_6['urban population'] + df_food_6['GDD_rural_median'] * df_food_6['rural population'])) * df_food_6['HANPP_embodied_in_trade']
df_food_6['FeH_rural_median'] = ((df_food_6['GDD_rural_median'] * df_food_6['rural population']) / (df_food_6['GDD_urban_median'] * df_food_6['urban population'] + df_food_6['GDD_rural_median'] * df_food_6['rural population'])) * df_food_6['HANPP_embodied_in_trade']
df_food_6['FeH_urban_cap_median'] =  df_food_6['FeH_urban_median'] / df_food_6['urban population']
df_food_6['FeH_rural_cap_median'] = df_food_6['FeH_rural_median'].div(df_food_6['rural population'].where(df_food_6['rural population'] != 0, np.nan))
df_food_6[['FeH_urban_median', 'FeH_rural_median','FeH_urban_cap_median','FeH_rural_cap_median']] = df_food_6[['FeH_urban_median', 'FeH_rural_median','FeH_urban_cap_median','FeH_rural_cap_median']].apply(pd.to_numeric)

#high estimate: urban lower, rural upper
df_food_6['FeH_urban_hoch'] = ((df_food_6['GDD_urban_lower'] * df_food_6['urban population']) / (df_food_6['GDD_urban_lower'] * df_food_6['urban population'] + df_food_6['GDD_rural_upper'] * df_food_6['rural population'])) * df_food_6['HANPP_embodied_in_trade']
df_food_6['FeH_rural_hoch'] = ((df_food_6['GDD_rural_upper'] * df_food_6['rural population']) / (df_food_6['GDD_urban_lower'] * df_food_6['urban population'] + df_food_6['GDD_rural_upper'] * df_food_6['rural population'])) * df_food_6['HANPP_embodied_in_trade']
df_food_6['FeH_urban_cap_hoch'] =  df_food_6['FeH_urban_hoch'] / df_food_6['urban population']
df_food_6['FeH_rural_cap_hoch'] = df_food_6['FeH_rural_hoch'].div(df_food_6['rural population'].where(df_food_6['rural population'] != 0, np.nan))
df_food_6[['FeH_urban_hoch', 'FeH_rural_hoch','FeH_urban_cap_hoch','FeH_rural_cap_hoch']] = df_food_6[['FeH_urban_hoch', 'FeH_rural_hoch','FeH_urban_cap_hoch','FeH_rural_cap_hoch']].apply(pd.to_numeric)

#low estimate: urban upper, rural lower
df_food_6['FeH_urban_niedrig'] = ((df_food_6['GDD_urban_upper'] * df_food_6['urban population']) / (df_food_6['GDD_urban_upper'] * df_food_6['urban population'] + df_food_6['GDD_rural_lower'] * df_food_6['rural population'])) * df_food_6['HANPP_embodied_in_trade']
df_food_6['FeH_rural_niedrig'] = ((df_food_6['GDD_rural_lower'] * df_food_6['rural population']) / (df_food_6['GDD_urban_upper'] * df_food_6['urban population'] + df_food_6['GDD_rural_lower'] * df_food_6['rural population'])) * df_food_6['HANPP_embodied_in_trade']
df_food_6['FeH_urban_cap_niedrig'] =  df_food_6['FeH_urban_niedrig'] / df_food_6['urban population']
df_food_6['FeH_rural_cap_niedrig'] = df_food_6['FeH_rural_niedrig'].div(df_food_6['rural population'].where(df_food_6['rural population'] != 0, np.nan))
df_food_6[['FeH_urban_niedrig', 'FeH_rural_niedrig','FeH_urban_cap_niedrig','FeH_rural_cap_niedrig']] = df_food_6[['FeH_urban_niedrig', 'FeH_rural_niedrig','FeH_urban_cap_niedrig','FeH_rural_cap_niedrig']].apply(pd.to_numeric)

#urban/rural kcal median,high,low
df_food_6['kcal_urban_median'] = ((df_food_6['GDD_urban_median'] * df_food_6['urban population']) / (df_food_6['GDD_urban_median'] * df_food_6['urban population'] + df_food_6['GDD_rural_median'] * df_food_6['rural population'])) * df_food_6['kcal_traded']
df_food_6['kcal_rural_median'] = ((df_food_6['GDD_rural_median'] * df_food_6['rural population']) / (df_food_6['GDD_urban_median'] * df_food_6['urban population'] + df_food_6['GDD_rural_median'] * df_food_6['rural population'])) * df_food_6['kcal_traded']
df_food_6['kcal_urb_cap_median'] =  (df_food_6['kcal_urban_median'] / df_food_6['urban population']) / 365
df_food_6['kcal_rur_cap_median'] = df_food_6.kcal_rural_median.div(df_food_6['rural population'].where(df_food_6['rural population'] != 0, np.nan))
df_food_6['kcal_rur_cap_median'] = df_food_6['kcal_rur_cap_median'] / 365

#low: urban upper, rural lower
df_food_6['kcal_urban_niedrig'] = ((df_food_6['GDD_urban_upper'] * df_food_6['urban population']) / (df_food_6['GDD_urban_upper'] * df_food_6['urban population'] + df_food_6['GDD_rural_lower'] * df_food_6['rural population'])) * df_food_6['kcal_traded']
df_food_6['kcal_rural_niedrig'] = ((df_food_6['GDD_rural_lower'] * df_food_6['rural population']) / (df_food_6['GDD_urban_upper'] * df_food_6['urban population'] + df_food_6['GDD_rural_lower'] * df_food_6['rural population'])) * df_food_6['kcal_traded']

#high: urban lower, rural upper
df_food_6['kcal_urban_hoch'] = ((df_food_6['GDD_urban_lower'] * df_food_6['urban population']) / (df_food_6['GDD_urban_lower'] * df_food_6['urban population'] + df_food_6['GDD_rural_upper'] * df_food_6['rural population'])) * df_food_6['kcal_traded']
df_food_6['kcal_rural_hoch'] = ((df_food_6['GDD_rural_upper'] * df_food_6['rural population']) / (df_food_6['GDD_urban_lower'] * df_food_6['urban population'] + df_food_6['GDD_rural_upper'] * df_food_6['rural population'])) * df_food_6['kcal_traded']

###############################################################################
#National Dataframe
##for summing up nans should be 0:
df_food_national = df_food_6.fillna(0)
df_food_national = df_food_national.groupby(['Destination_code_FAO','Destination','income_group',
                                      'Year','Final_use','food_group']).sum(['HANPP_embodied_in_trade',
                                                                             'tonnes_traded_dm','kcal_traded',
                                                                             'kcal/cap/day',
                                                                             'FeH_urban_median','FeH_rural_median','FeH_urban_cap_median','FeH_rural_cap_median',
                                                                             'FeH_urban_hoch','FeH_rural_hoch','FeH_urban_cap_hoch','FeH_rural_cap_hoch',
                                                                             'FeH_urban_niedrig','FeH_rural_niedrig','FeH_urban_cap_niedrig','FeH_rural_cap_niedrig',
                                                                             'kcal_urban_median', 'kcal_rural_median',
                                                                             'kcal_urb_cap_median', 'kcal_urb_cap_median',
                                                                             'kcal_urban_niedrig','kcal_rural_niedrig',
                                                                             'kcal_urban_hoch','kcal_rural_hoch']).reset_index()
                                                    
df_food_national = df_food_national.drop(['GDD_urban_median','GDD_rural_median',
                                          'GDD_urban_upper','GDD_rural_upper',
                                          'GDD_urban_lower','GDD_rural_lower',
                                          'pop_urb_share','pop_national','urban population','rural population'], axis=1)


#Regional Dataframe
df_food_regional = df_food_national.groupby(['income_group','Year','Final_use','food_group']).sum(['HANPP_embodied_in_trade',
                                                                             'tonnes_traded_dm','kcal_traded',
                                                                             'FeH_urban_median','FeH_rural_median',
                                                                             'FeH_urban_hoch','FeH_rural_hoch',
                                                                             'FeH_urban_niedrig','FeH_rural_niedrig',
                                                                             'kcal_urban_median', 'kcal_rural_median',
                                                                             'kcal_urban_niedrig','kcal_rural_niedrig',
                                                                             'kcal_urban_hoch','kcal_rural_hoch']).reset_index()

df_food_regional = df_food_regional.drop(['Destination_code_FAO',
                                          'FeH_urban_cap_median','FeH_rural_cap_median',
                                          'FeH_urban_cap_hoch','FeH_rural_cap_hoch',
                                          'FeH_urban_cap_niedrig','FeH_rural_cap_niedrig',
                                          'kcal/cap/day','kcal_rur_cap_median','kcal_urb_cap_median'], axis=1)

#3-year-average:
columns_to_average = ['kcal_traded','HANPP_embodied_in_trade','FeH_urban_median','FeH_rural_median',
                      'FeH_urban_niedrig','FeH_rural_niedrig','FeH_urban_hoch','FeH_rural_hoch',
                      'kcal_urban_median', 'kcal_rural_median','kcal_urban_niedrig','kcal_rural_niedrig',
                      'kcal_urban_hoch','kcal_rural_hoch']

# Calculate the rolling mean and differentiate between groups
for column in columns_to_average:
    df_food_regional[f'{column}_avg'] = df_food_regional.groupby(['income_group', 'Final_use', 'food_group'])[column].transform(
        lambda x: x.rolling(window=3, center=True, min_periods=1).mean()
    )
# Fill NaN values with original column values for boundary years (first and last)
for column in columns_to_average:
    df_food_regional[f'{column}_avg'].fillna(df_food_regional[column], inplace=True)
df_food_regional.drop(columns=columns_to_average, inplace=True)
# Rename the new columns back to their original names if desired
df_food_regional.rename(columns={f'{column}_avg': column for column in columns_to_average}, inplace=True)


#add population to calculate per capita values
df_food_regional = df_food_regional.merge(df_pop_reg, how='left', on=['income_group','Year'])

df_food_regional['FeH_urban_cap_median'] = df_food_regional['FeH_urban_median'] / df_food_regional['urban population']
df_food_regional['FeH_rural_cap_median'] = df_food_regional['FeH_rural_median'] / df_food_regional['rural population']
df_food_regional['FeH_urban_cap_hoch'] = df_food_regional['FeH_urban_hoch'] / df_food_regional['urban population']
df_food_regional['FeH_rural_cap_hoch'] = df_food_regional['FeH_rural_hoch'] / df_food_regional['rural population']
df_food_regional['FeH_urban_cap_niedrig'] = df_food_regional['FeH_urban_niedrig'] / df_food_regional['urban population']
df_food_regional['FeH_rural_cap_niedrig'] = df_food_regional['FeH_rural_niedrig'] / df_food_regional['rural population']

df_food_regional['kcal/cap/day'] = df_food_regional['kcal_traded'] / df_food_regional['pop_regional'] /365

df_food_regional['kcal_urban_cap_median'] = df_food_regional['kcal_urban_median'] / df_food_regional['urban population'] /365
df_food_regional['kcal_rural_cap_median'] = df_food_regional['kcal_rural_median'] / df_food_regional['rural population'] / 365

df_food_regional['kcal_urban_cap_niedrig'] = df_food_regional['kcal_urban_niedrig'] / df_food_regional['urban population'] /365
df_food_regional['kcal_rural_cap_niedrig'] = df_food_regional['kcal_rural_niedrig'] / df_food_regional['rural population'] / 365

df_food_regional['kcal_urban_cap_hoch'] = df_food_regional['kcal_urban_hoch'] / df_food_regional['urban population'] /365
df_food_regional['kcal_rural_cap_hoch'] = df_food_regional['kcal_rural_hoch'] / df_food_regional['rural population'] / 365

###############################################################################
#Dataframes for Figure 3abc
#stacked regions in pop
df_food_regions_pop = df_food_regional[['income_group','Year','pop_regional']].drop_duplicates()
df_food_regions_pop = df_food_regions_pop.groupby(['income_group','Year']).sum(['pop_regional']).reset_index()
df_food_regions_pop = df_food_regions_pop.pivot(index = 'Year', columns='income_group', values='pop_regional')
df_food_regions_pop = df_food_regions_pop / 1000 /1000/1000  # to billion
df_food_regions_pop = df_food_regions_pop.rename(columns={'H': 'High-income',
                                                          'L': 'Low-income',
                                                          'LM': 'Lower-middle-income',
                                                          'UM': 'Upper-middle-income'})
df_food_regions_pop = df_food_regions_pop[['Low-income', 'Lower-middle-income','Upper-middle-income','High-income']]
#stacked regions in FeH
df_food_regions_FeH = df_food_regional[['income_group','Year','HANPP_embodied_in_trade']]
df_food_regions_FeH = df_food_regions_FeH.groupby(['income_group','Year']).sum(['HANPP_embodied_in_trade']).reset_index()
df_food_regions_FeH = df_food_regions_FeH.pivot(index = 'Year', columns='income_group', values='HANPP_embodied_in_trade')
df_food_regions_FeH = df_food_regions_FeH / 1000 /1000/1000 # to Gt
df_food_regions_FeH = df_food_regions_FeH.rename(columns={'H': 'High-income',
                                                          'L': 'Low-income',
                                                          'LM': 'Lower-middle-income',
                                                          'UM': 'Upper-middle-income'})
df_food_regions_FeH = df_food_regions_FeH[['Low-income', 'Lower-middle-income','Upper-middle-income','High-income']]
#intensities regions FeH/cap
df_food_regions_pop_glo = df_food_regions_pop.copy()
df_food_regions_pop_glo['Global'] = df_food_regions_pop_glo.sum(axis=1)

df_food_regions_FeH_glo = df_food_regions_FeH.copy()
df_food_regions_FeH_glo['Global'] = df_food_regions_FeH_glo.sum(axis=1)

df_food_regions_int = df_food_regions_FeH_glo/df_food_regions_pop_glo
df_food_regions_int = df_food_regions_int[['Low-income', 'Lower-middle-income','Upper-middle-income','High-income','Global']]
###############################################################################
#Dataframes for Figure 3def
#stacked food group in kcal
df_food_group_global_kcal = df_food_regional[['income_group','Year','food_group','kcal_traded']]
df_food_group_global_kcal = df_food_group_global_kcal.groupby(['Year','food_group']).sum(['kcal_traded']).reset_index()
df_food_group_global_kcal = df_food_group_global_kcal.pivot(index = 'Year', columns='food_group', values='kcal_traded')
df_food_group_global_kcal['Livestock products'] = df_food_group_global_kcal['Ruminant meat'] + df_food_group_global_kcal['Monogastric products'] + df_food_group_global_kcal['Milk and milk products']  
df_food_group_global_kcal['overall'] = df_food_group_global_kcal.sum(axis=1)
df_food_group_global_kcal = df_food_group_global_kcal[['Livestock products','Cereals','Tubers and legumes','Oil seeds and nuts','Fruits and Vegetables','Sugars and stimulants', 'overall']]

#stacked food group in FeH
df_food_group_global_FeH = df_food_regional[['income_group','Year','food_group','HANPP_embodied_in_trade']]
df_food_group_global_FeH = df_food_group_global_FeH.groupby(['Year','food_group']).sum(['HANPP_embodied_in_trade']).reset_index()
df_food_group_global_FeH = df_food_group_global_FeH.pivot(index = 'Year', columns='food_group', values='HANPP_embodied_in_trade')
df_food_group_global_FeH['Livestock products'] = df_food_group_global_FeH['Ruminant meat'] + df_food_group_global_FeH['Monogastric products'] + df_food_group_global_FeH['Milk and milk products']  
df_food_group_global_FeH['overall'] = df_food_group_global_FeH.sum(axis=1)

#Rename Infrastructure:
df_food_group_global_FeH = df_food_group_global_FeH.rename(columns={'Infra': 'Embodied built-up land'})
#Reorder columns:
df_food_group_global_FeH = df_food_group_global_FeH[['Livestock products','Cereals','Tubers and legumes','Oil seeds and nuts','Fruits and Vegetables','Sugars and stimulants','Embodied built-up land', 'overall']]

#intensities food group FeH/kcal
df_food_group_global_int = df_food_group_global_FeH/df_food_group_global_kcal 

#Delete overall/infrastrcuture from kcal and int columns:
df_food_group_global_kcal = df_food_group_global_kcal[['Livestock products','Cereals','Tubers and legumes','Oil seeds and nuts','Fruits and Vegetables','Sugars and stimulants']]
df_food_group_global_int = df_food_group_global_int[['Livestock products','Cereals','Tubers and legumes','Oil seeds and nuts','Fruits and Vegetables','Sugars and stimulants','overall']]
#delete overall from FeH
df_food_group_global_FeH = df_food_group_global_FeH[['Livestock products','Cereals','Tubers and legumes','Oil seeds and nuts','Fruits and Vegetables','Sugars and stimulants','Embodied built-up land']]

df_food_group_global_kcal = df_food_group_global_kcal/1000/1000/1000/1000/1000 # to Exacal
df_food_group_global_FeH = df_food_group_global_FeH / 1000 /1000/1000 # to Gt
df_food_group_global_int = df_food_group_global_int *1000 *1000 #from t/kcal to g/kcal

###############################################################################
#Dataframes for Figure 3ghi
#stacked regions in urb/rur
df_food_urbrur_pop = df_food_regional[['income_group','Year','urban population','rural population']].drop_duplicates()
df_food_urbrur_pop = df_food_urbrur_pop.groupby('Year').sum(['urban population','rural population']).reset_index()
df_food_urbrur_pop = df_food_urbrur_pop.set_index('Year')
df_food_urbrur_pop = df_food_urbrur_pop.rename(columns={'urban population': 'urban','rural population': 'rural'})
df_food_urbrur_pop = df_food_urbrur_pop / 1000 /1000/1000  # to billion
df_food_urbrur_pop = df_food_urbrur_pop[['rural','urban']]

#stacked regions in FeH
#median
df_food_urbrur_FeH = df_food_regional[['income_group','Year','FeH_urban_median','FeH_rural_median']]
df_food_urbrur_FeH = df_food_urbrur_FeH.groupby('Year').sum(['FeH_urban_median','FeH_rural_median']).reset_index()
df_food_urbrur_FeH = df_food_urbrur_FeH.set_index('Year')
df_food_urbrur_FeH = df_food_urbrur_FeH.rename(columns={'FeH_urban_median': 'urban','FeH_rural_median': 'rural'})
df_food_urbrur_FeH = df_food_urbrur_FeH / 1000 /1000/1000 # to Gt
df_food_urbrur_FeH = df_food_urbrur_FeH[['rural','urban']]

#intensities urban/rural FeH/cap including uncertainty
df_food_urbrur_FeH_unc = df_food_regional[['income_group','Year','FeH_urban_median','FeH_rural_median','FeH_urban_niedrig','FeH_rural_niedrig',
                                           'FeH_urban_hoch','FeH_rural_hoch']]
df_food_urbrur_FeH_unc = df_food_urbrur_FeH_unc.groupby('Year').sum(['FeH_urban_median','FeH_rural_median','FeH_urban_niedrig','FeH_rural_niedrig',
                                           'FeH_urban_hoch','FeH_rural_hoch']).reset_index()
df_food_urbrur_FeH_unc = df_food_urbrur_FeH_unc.set_index('Year')
df_food_urbrur_FeH_unc = df_food_urbrur_FeH_unc / 1000 /1000/1000 # to Gt

df_food_urbrur_FeH_unc = df_food_urbrur_FeH_unc.rename(columns={'FeH_urban_median': 'urban','FeH_rural_median': 'rural',
                                                                'FeH_urban_hoch': 'urban-lower boundary','FeH_rural_hoch': 'rural-upper boundary',
                                                                'FeH_urban_niedrig': 'urban-upper boundary','FeH_rural_niedrig': 'rural-lower boundary'})

df_food_urbrur_FeH_unc['global'] = df_food_urbrur_FeH_unc['urban'] + df_food_urbrur_FeH_unc['rural']

df_food_urbrur_pop_glo = df_food_urbrur_pop.copy()
df_food_urbrur_pop_glo['global'] = df_food_urbrur_pop_glo.sum(axis=1)
df_food_urbrur_pop_glo['urban-lower boundary'] = df_food_urbrur_pop_glo['urban']
df_food_urbrur_pop_glo['rural-lower boundary'] = df_food_urbrur_pop_glo['rural']
df_food_urbrur_pop_glo['rural-upper boundary'] = df_food_urbrur_pop_glo['rural']
df_food_urbrur_pop_glo['urban-upper boundary'] = df_food_urbrur_pop_glo['urban']

df_food_urbrur_int = df_food_urbrur_FeH_unc/df_food_urbrur_pop_glo 
###############################################################################
#Dataframe for Figure 4a: Urban/Rural Food-eHANPP per capita
df_food_reg_urbrur_cap_FeH = df_food_regional.groupby(['income_group','Year']).sum().reset_index()
df_food_reg_urbrur_cap_FeH = df_food_reg_urbrur_cap_FeH[['income_group','Year','FeH_urban_median','FeH_rural_median']]

income_group_mapping = {'L': 'Low-income',
                        'LM': 'Lower-middle-income',
                        'UM': 'Upper-middle-income',
                        'H': 'High-income'}
income_group_order = ['Low-income', 'Lower-middle-income', 'Upper-middle-income', 'High-income', 'Global']
#1990
df_food_reg_urbrur_cap_FeH_1990 = df_food_reg_urbrur_cap_FeH.loc[df_food_reg_urbrur_cap_FeH['Year'].isin([1990])]
#addpop
df_food_reg_urbrur_cap_FeH_1990 = df_food_reg_urbrur_cap_FeH_1990.merge(df_pop_reg, how = 'left', on = ['income_group','Year'])
#add global
df_food_reg_urbrur_cap_FeH_1990.loc[7]= df_food_reg_urbrur_cap_FeH_1990.sum()
df_food_reg_urbrur_cap_FeH_1990.at[7, 'income_group'] = 'Global'
df_food_reg_urbrur_cap_FeH_1990['urban_FeH_cap'] = df_food_reg_urbrur_cap_FeH_1990['FeH_urban_median'] / df_food_reg_urbrur_cap_FeH_1990['urban population']
df_food_reg_urbrur_cap_FeH_1990['rural_FeH_cap'] = df_food_reg_urbrur_cap_FeH_1990['FeH_rural_median'] / df_food_reg_urbrur_cap_FeH_1990['rural population']
df_food_reg_urbrur_cap_FeH_1990 = df_food_reg_urbrur_cap_FeH_1990.set_index('income_group')
df_food_reg_urbrur_cap_FeH_1990 = df_food_reg_urbrur_cap_FeH_1990[['urban_FeH_cap','rural_FeH_cap']]
df_food_reg_urbrur_cap_FeH_1990 = df_food_reg_urbrur_cap_FeH_1990.rename(index=income_group_mapping)
df_food_reg_urbrur_cap_FeH_1990 = df_food_reg_urbrur_cap_FeH_1990.loc[income_group_order]

#2019
df_food_reg_urbrur_cap_FeH_2019 = df_food_reg_urbrur_cap_FeH.loc[df_food_reg_urbrur_cap_FeH['Year'].isin([2019])]
#addpop
df_food_reg_urbrur_cap_FeH_2019 = df_food_reg_urbrur_cap_FeH_2019.merge(df_pop_reg, how = 'left', on = ['income_group','Year'])
#add global
df_food_reg_urbrur_cap_FeH_2019.loc[7]= df_food_reg_urbrur_cap_FeH_2019.sum()
df_food_reg_urbrur_cap_FeH_2019.at[7, 'income_group'] = 'Global'
df_food_reg_urbrur_cap_FeH_2019['urban_FeH_cap'] = df_food_reg_urbrur_cap_FeH_2019['FeH_urban_median'] / df_food_reg_urbrur_cap_FeH_2019['urban population']
df_food_reg_urbrur_cap_FeH_2019['rural_FeH_cap'] = df_food_reg_urbrur_cap_FeH_2019['FeH_rural_median'] / df_food_reg_urbrur_cap_FeH_2019['rural population']
df_food_reg_urbrur_cap_FeH_2019 = df_food_reg_urbrur_cap_FeH_2019.set_index('income_group')
df_food_reg_urbrur_cap_FeH_2019 = df_food_reg_urbrur_cap_FeH_2019[['urban_FeH_cap','rural_FeH_cap']]
df_food_reg_urbrur_cap_FeH_2019 = df_food_reg_urbrur_cap_FeH_2019.rename(index=income_group_mapping)
df_food_reg_urbrur_cap_FeH_2019 = df_food_reg_urbrur_cap_FeH_2019.loc[income_group_order]

###############################################################################
#Dataframe for Figure 4b: Urban/Rural Food-eHANPP intensity
df_food_reg_urbrur_FeHint_cap = df_food_regional.groupby(['income_group','Year']).sum().reset_index()
df_food_reg_urbrur_FeHint_cap = df_food_reg_urbrur_FeHint_cap[['income_group','Year','FeH_urban_median','FeH_rural_median','kcal_urban_median','kcal_rural_median']]

#1990
df_food_reg_urbrur_FeHint_cap_1990 = df_food_reg_urbrur_FeHint_cap.loc[df_food_reg_urbrur_FeHint_cap['Year'].isin([1990])]
#add global
df_food_reg_urbrur_FeHint_cap_1990.loc[7]= df_food_reg_urbrur_FeHint_cap_1990.sum()
df_food_reg_urbrur_FeHint_cap_1990.at[7, 'income_group'] = 'Global'
df_food_reg_urbrur_FeHint_cap_1990['urban_FeHint_cap'] = df_food_reg_urbrur_FeHint_cap_1990['FeH_urban_median'] / df_food_reg_urbrur_FeHint_cap_1990['kcal_urban_median']
df_food_reg_urbrur_FeHint_cap_1990['rural_FeHint_cap'] = df_food_reg_urbrur_FeHint_cap_1990['FeH_rural_median'] / df_food_reg_urbrur_FeHint_cap_1990['kcal_rural_median']
df_food_reg_urbrur_FeHint_cap_1990 = df_food_reg_urbrur_FeHint_cap_1990.set_index('income_group')
df_food_reg_urbrur_FeHint_cap_1990 = df_food_reg_urbrur_FeHint_cap_1990[['urban_FeHint_cap','rural_FeHint_cap']]
df_food_reg_urbrur_FeHint_cap_1990 = df_food_reg_urbrur_FeHint_cap_1990  * 1000 * 1000  #Umrechnung von t/kcal in g/kcal
df_food_reg_urbrur_FeHint_cap_1990 = df_food_reg_urbrur_FeHint_cap_1990.rename(index=income_group_mapping)
df_food_reg_urbrur_FeHint_cap_1990 = df_food_reg_urbrur_FeHint_cap_1990.loc[income_group_order]

#2019
df_food_reg_urbrur_FeHint_cap_2019 = df_food_reg_urbrur_FeHint_cap.loc[df_food_reg_urbrur_FeHint_cap['Year'].isin([2019])]
#add global
df_food_reg_urbrur_FeHint_cap_2019.loc[7]= df_food_reg_urbrur_FeHint_cap_2019.sum()
df_food_reg_urbrur_FeHint_cap_2019.at[7, 'income_group'] = 'Global'
df_food_reg_urbrur_FeHint_cap_2019['urban_FeHint_cap'] = df_food_reg_urbrur_FeHint_cap_2019['FeH_urban_median'] / df_food_reg_urbrur_FeHint_cap_2019['kcal_urban_median']
df_food_reg_urbrur_FeHint_cap_2019['rural_FeHint_cap'] = df_food_reg_urbrur_FeHint_cap_2019['FeH_rural_median'] / df_food_reg_urbrur_FeHint_cap_2019['kcal_rural_median']
df_food_reg_urbrur_FeHint_cap_2019 = df_food_reg_urbrur_FeHint_cap_2019.set_index('income_group')
df_food_reg_urbrur_FeHint_cap_2019 = df_food_reg_urbrur_FeHint_cap_2019[['urban_FeHint_cap','rural_FeHint_cap']]
df_food_reg_urbrur_FeHint_cap_2019 = df_food_reg_urbrur_FeHint_cap_2019  * 1000 * 1000  #Umrechnung von t/kcal in g/kcal
df_food_reg_urbrur_FeHint_cap_2019 = df_food_reg_urbrur_FeHint_cap_2019.rename(index=income_group_mapping)
df_food_reg_urbrur_FeHint_cap_2019 = df_food_reg_urbrur_FeHint_cap_2019.loc[income_group_order]

###############################################################################
#Dataframe for Figure c: Urban/Rural livestock supply
livestock_products = ['Milk and milk products','Monogastric products','Ruminant meat']
df_food_reg_urbrur_cap_live = df_food_regional[['income_group','Year','food_group','kcal_urban_median','kcal_rural_median']]
df_food_reg_urbrur_cap_live = df_food_reg_urbrur_cap_live.loc[df_food_reg_urbrur_cap_live['food_group'].isin(livestock_products)]
df_food_reg_urbrur_cap_live = df_food_reg_urbrur_cap_live.groupby(['income_group','Year']).sum().reset_index()

#1990
df_food_reg_urbrur_cap_live_1990 = df_food_reg_urbrur_cap_live.loc[df_food_reg_urbrur_cap_live['Year'].isin([1990])]
#addpop
df_food_reg_urbrur_cap_live_1990 = df_food_reg_urbrur_cap_live_1990.merge(df_pop_reg, how = 'left', on = ['income_group','Year'])
#add global
df_food_reg_urbrur_cap_live_1990.loc[7]= df_food_reg_urbrur_cap_live_1990.sum()
df_food_reg_urbrur_cap_live_1990.at[7, 'income_group'] = 'Global'
df_food_reg_urbrur_cap_live_1990['urban_live_cap'] = (df_food_reg_urbrur_cap_live_1990['kcal_urban_median'] / df_food_reg_urbrur_cap_live_1990['urban population']) /365
df_food_reg_urbrur_cap_live_1990['rural_live_cap'] = (df_food_reg_urbrur_cap_live_1990['kcal_rural_median'] / df_food_reg_urbrur_cap_live_1990['rural population']) /365
df_food_reg_urbrur_cap_live_1990 = df_food_reg_urbrur_cap_live_1990.set_index('income_group')
df_food_reg_urbrur_cap_live_1990 = df_food_reg_urbrur_cap_live_1990[['urban_live_cap','rural_live_cap']]
df_food_reg_urbrur_cap_live_1990 = df_food_reg_urbrur_cap_live_1990.rename(index=income_group_mapping)
df_food_reg_urbrur_cap_live_1990 = df_food_reg_urbrur_cap_live_1990.loc[income_group_order]

#2019
df_food_reg_urbrur_cap_live_2019 = df_food_reg_urbrur_cap_live.loc[df_food_reg_urbrur_cap_live['Year'].isin([2019])]
#addpop
df_food_reg_urbrur_cap_live_2019 = df_food_reg_urbrur_cap_live_2019.merge(df_pop_reg, how = 'left', on = ['income_group','Year'])
#add global
df_food_reg_urbrur_cap_live_2019.loc[7]= df_food_reg_urbrur_cap_live_2019.sum()
df_food_reg_urbrur_cap_live_2019.at[7, 'income_group'] = 'Global'
df_food_reg_urbrur_cap_live_2019['urban_live_cap'] = (df_food_reg_urbrur_cap_live_2019['kcal_urban_median'] / df_food_reg_urbrur_cap_live_2019['urban population']) / 365
df_food_reg_urbrur_cap_live_2019['rural_live_cap'] = (df_food_reg_urbrur_cap_live_2019['kcal_rural_median'] / df_food_reg_urbrur_cap_live_2019['rural population'])/ 365
df_food_reg_urbrur_cap_live_2019 = df_food_reg_urbrur_cap_live_2019.set_index('income_group')
df_food_reg_urbrur_cap_live_2019 = df_food_reg_urbrur_cap_live_2019[['urban_live_cap','rural_live_cap']]
df_food_reg_urbrur_cap_live_2019 = df_food_reg_urbrur_cap_live_2019.rename(index=income_group_mapping)
df_food_reg_urbrur_cap_live_2019 = df_food_reg_urbrur_cap_live_2019.loc[income_group_order]
###############################################################################
#Dataframe for Figure 3d: Urban/rural plant-based supply

df_food_reg_urbrur_cap_plant = df_food_regional[['income_group','Year','food_group','kcal_urban_median','kcal_rural_median']]
df_food_reg_urbrur_cap_plant = df_food_reg_urbrur_cap_plant.loc[~df_food_reg_urbrur_cap_plant['food_group'].isin(livestock_products)]
df_food_reg_urbrur_cap_plant = df_food_reg_urbrur_cap_plant.groupby(['income_group','Year']).sum().reset_index()

#1990
df_food_reg_urbrur_cap_plant_1990 = df_food_reg_urbrur_cap_plant.loc[df_food_reg_urbrur_cap_plant['Year'].isin([1990])]
#addpop
df_food_reg_urbrur_cap_plant_1990 = df_food_reg_urbrur_cap_plant_1990.merge(df_pop_reg, how = 'left', on = ['income_group','Year'])
#add global
df_food_reg_urbrur_cap_plant_1990.loc[7]= df_food_reg_urbrur_cap_plant_1990.sum()
df_food_reg_urbrur_cap_plant_1990.at[7, 'income_group'] = 'Global'
df_food_reg_urbrur_cap_plant_1990['urban_plant_cap'] = (df_food_reg_urbrur_cap_plant_1990['kcal_urban_median'] / df_food_reg_urbrur_cap_plant_1990['urban population']) /365
df_food_reg_urbrur_cap_plant_1990['rural_plant_cap'] = (df_food_reg_urbrur_cap_plant_1990['kcal_rural_median'] / df_food_reg_urbrur_cap_plant_1990['rural population']) /365
df_food_reg_urbrur_cap_plant_1990 = df_food_reg_urbrur_cap_plant_1990.set_index('income_group')
df_food_reg_urbrur_cap_plant_1990 = df_food_reg_urbrur_cap_plant_1990[['urban_plant_cap','rural_plant_cap']]
df_food_reg_urbrur_cap_plant_1990 = df_food_reg_urbrur_cap_plant_1990.rename(index=income_group_mapping)
df_food_reg_urbrur_cap_plant_1990 = df_food_reg_urbrur_cap_plant_1990.loc[income_group_order]

#2019
df_food_reg_urbrur_cap_plant_2019 = df_food_reg_urbrur_cap_plant.loc[df_food_reg_urbrur_cap_plant['Year'].isin([2019])]
#addpop
df_food_reg_urbrur_cap_plant_2019 = df_food_reg_urbrur_cap_plant_2019.merge(df_pop_reg, how = 'left', on = ['income_group','Year'])
#add global
df_food_reg_urbrur_cap_plant_2019.loc[7]= df_food_reg_urbrur_cap_plant_2019.sum()
df_food_reg_urbrur_cap_plant_2019.at[7, 'income_group'] = 'Global'
df_food_reg_urbrur_cap_plant_2019['urban_plant_cap'] = (df_food_reg_urbrur_cap_plant_2019['kcal_urban_median'] / df_food_reg_urbrur_cap_plant_2019['urban population']) / 365
df_food_reg_urbrur_cap_plant_2019['rural_plant_cap'] = (df_food_reg_urbrur_cap_plant_2019['kcal_rural_median'] / df_food_reg_urbrur_cap_plant_2019['rural population'])/ 365
df_food_reg_urbrur_cap_plant_2019 = df_food_reg_urbrur_cap_plant_2019.set_index('income_group')
df_food_reg_urbrur_cap_plant_2019 = df_food_reg_urbrur_cap_plant_2019[['urban_plant_cap','rural_plant_cap']]
df_food_reg_urbrur_cap_plant_2019 = df_food_reg_urbrur_cap_plant_2019.rename(index=income_group_mapping)
df_food_reg_urbrur_cap_plant_2019 = df_food_reg_urbrur_cap_plant_2019.loc[income_group_order]
###############################################################################
#                               PLOTS                                         #
###############################################################################

colors_food_groups = ['#833C0C','#FFC000','#F4B084','#9BC2E6','#F290DF','#AEAAAA','#44546A']
colors_food_groupsandall = ['#833C0C','#FFC000','#F4B084','#9BC2E6','#F290DF','#AEAAAA','black']
colors_regions = ['#F4D03F','#2874A6','#A93226','#73C6B6']  
colors_regionsandglo = ['#F4D03F','#2874A6','#A93226','#73C6B6','black']
colors_FeH_urb_rur = ['#00B050','#808080']
colors_FeH_urb_rur_glo = ['black','#00B050','#00B050','#00B050','#808080','#808080','#808080']

#PLOT: Figure 3
dataframes_fig3 = [df_food_regions_pop, df_food_regions_FeH, df_food_regions_int, 
                   df_food_group_global_kcal, df_food_group_global_FeH, df_food_group_global_int,
                   df_food_urbrur_pop, df_food_urbrur_FeH, df_food_urbrur_int]

colors_fig3 = [colors_regions,colors_regions,colors_regionsandglo,
               colors_food_groups,colors_food_groups,colors_food_groupsandall,
               colors_FeH_urb_rur,colors_FeH_urb_rur,colors_FeH_urb_rur_glo]


fig, axes = plt.subplots(3, 3, figsize=(15, 15)) # Define each subplot individually
# Subplot 1
df1 = dataframes_fig3[0]
colors1 = colors_fig3[0]
ax1 = axes[0,0]
df1.plot(kind='area', ax=ax1, xlim=(1990, 2020), ylim=(0, 8), xlabel='',
         grid=True, color=colors1, linewidth=0, legend=False)
ax1.set_title('a) Population by region', fontsize=15)
ax1.set_ylabel('billion', fontsize=15)
ax1.set_xlabel('', fontsize=15)
ax1.tick_params(axis='x', labelsize=15)
ax1.tick_params(axis='y', labelsize=15)
ax1.set_xticks([1990, 2000, 2010, 2020])
ax1.set_yticks([0, 2, 4, 6, 8])
ax1.grid(color='black', linewidth=0.3, alpha=0.2)
groups1 = df1.columns
cumulative1 = df1.cumsum(axis=1)
for group in groups1:
    x_pos = 2020
    if x_pos in df1.index:
        y_pos = (cumulative1[group] - df1[group] / 2).loc[x_pos]
        ax1.text(x_pos, y_pos, group, fontsize=15, ha='right', va='center')
# Subplot 2
df2 = dataframes_fig3[1]
colors2 = colors_fig3[1]
ax2 = axes[0,1]
df2.plot(kind='area', ax=ax2, xlim=(1990, 2020), ylim=(0, 18), xlabel='',
         grid=True, color=colors1, linewidth=0, legend=False)
ax2.set_title('b) Food-eHANPP by region', fontsize=15)
ax2.set_ylabel('Gt dm/yr', fontsize=15)
ax2.set_xlabel('', fontsize=15)
ax2.tick_params(axis='x', labelsize=15)
ax2.tick_params(axis='y', labelsize=15)
ax2.set_xticks([1990, 2000, 2010, 2020])
ax2.set_yticks([0, 3, 6, 9, 12, 15, 18])
ax2.grid(color='black', linewidth=0.3, alpha=0.2)
groups2 = df2.columns
cumulative2 = df2.cumsum(axis=1)
for group in groups2:
    x_pos = 2020
    if x_pos in df2.index:
        y_pos = (cumulative2[group] - df2[group] / 2).loc[x_pos]
        ax2.text(x_pos, y_pos, group, fontsize=15, ha='right', va='center')            
# Subplot 3
df3 = dataframes_fig3[2]
colors3 = colors_fig3[2]
ax3 = axes[0,2]
df3.plot(kind='line', ax=ax3, xlim=(1990, 2020), ylim=(0, 6), xlabel='',
         grid=True, color=colors3, linewidth=4, legend=True)
ax3.set_title('c) Food-eHANPP per capita', fontsize=15)
ax3.legend(title='', loc = 'upper right', fontsize = 13)
ax3.set_ylabel('t dm/cap/yr', fontsize=15)
ax3.set_xlabel('', fontsize=15)
ax3.tick_params(axis='x', labelsize=15)
ax3.tick_params(axis='y', labelsize=15)
ax3.set_xticks([1990, 2000, 2010, 2020])
ax3.set_yticks([0,1,2,3,4,5,6])
ax3.grid(color='black', linewidth=0.3, alpha=0.2)
       
# Subplot 4
df4 = dataframes_fig3[3]
colors4 = colors_fig3[3]
ax4 = axes[1,0]
df4.plot(kind='area', ax=ax4, xlim=(1990, 2020), ylim=(0, 8), xlabel='',
         grid=True, color=colors4, linewidth=0, legend=False)
ax4.set_title('d) Food supply', fontsize=15)
ax4.set_ylabel('Ecal/yr', fontsize=15)
ax4.set_xlabel('', fontsize=15)
ax4.tick_params(axis='x', labelsize=15)
ax4.tick_params(axis='y', labelsize=15)
ax4.set_xticks([1990, 2000, 2010, 2020])
ax4.set_yticks([0, 2, 4, 6, 8, 10])
ax4.grid(color='black', linewidth=0.3, alpha=0.2)
groups4 = df4.columns
cumulative4 = df4.cumsum(axis=1)
for group in groups4:
    x_pos = 2020
    if x_pos in df4.index:
        y_pos = (cumulative4[group] - df4[group] / 2).loc[x_pos]
        ax4.text(x_pos, y_pos, group, fontsize=15, ha='right', va='center')
# Subplot 5
df5 = dataframes_fig3[4]
colors5 = colors_fig3[4]
ax5 = axes[1,1]
df5.plot(kind='area', ax=ax5, xlim=(1990, 2020), ylim=(0, 18), xlabel='',
         grid=True, color=colors5, linewidth=0, legend=False)
ax5.set_title('e) Food-eHANPP by food group', fontsize=15)
ax5.set_ylabel('Gt dm/yr', fontsize=15)
ax5.set_xlabel('', fontsize=15)
ax5.tick_params(axis='x', labelsize=15)
ax5.tick_params(axis='y', labelsize=15)
ax5.set_xticks([1990, 2000, 2010, 2020])
ax5.set_yticks([0, 3, 6, 9, 12, 15, 18])
ax5.grid(color='black', linewidth=0.3, alpha=0.2)
groups5 = df5.columns
cumulative5 = df5.cumsum(axis=1)
for group in groups5:
    x_pos = 2020
    if x_pos in df5.index:
        y_pos = (cumulative5[group] - df5[group] / 2).loc[x_pos]
        ax5.text(x_pos, y_pos, group, fontsize=15, ha='right', va='center')        
        
# Subplot 6
df6 = dataframes_fig3[5]
colors6 = colors_fig3[5]
ax6 = axes[1,2]
df6.plot(kind='line', ax=ax6, xlim=(1990, 2020), ylim=(0, 8), xlabel='',
         grid=True, color=colors6, linewidth=4, legend=True)
ax6.set_title('f) Food-eHANPP-intensity', fontsize=15)
ax6.legend(title='', loc = 'upper right', fontsize = 11)
ax6.set_ylabel('g dm/kcal', fontsize=15)
ax6.set_xlabel('', fontsize=15)
ax6.tick_params(axis='x', labelsize=15)
ax6.tick_params(axis='y', labelsize=15)
ax6.set_xticks([1990, 2000, 2010, 2020])
ax6.set_yticks([0, 2, 4, 6, 8, 10, 12])
ax6.grid(color='black', linewidth=0.3, alpha=0.2)        
        
# Subplot 7
df7 = dataframes_fig3[6]
colors7 = colors_fig3[6]
ax7 = axes[2,0]
df7.plot(kind='area', ax=ax7, xlim=(1990, 2020), ylim=(0, 8), xlabel='',
         grid=True, color=colors7, linewidth=0, legend=False)
ax7.set_title('g) Urban and rural population', fontsize=15)
ax7.set_ylabel('billion', fontsize=15)
ax7.set_xlabel('', fontsize=15)
ax7.tick_params(axis='x', labelsize=15)
ax7.tick_params(axis='y', labelsize=15)
ax7.set_xticks([1990, 2000, 2010, 2020])
ax7.set_yticks([0, 2, 4, 6, 8])
ax7.grid(color='black', linewidth=0.3, alpha=0.2)
groups7 = df7.columns
cumulative7 = df7.cumsum(axis=1)
for group in groups7:
    x_pos = 2020
    if x_pos in df7.index:
        y_pos = (cumulative7[group] - df7[group] / 2).loc[x_pos]
        ax7.text(x_pos, y_pos, group, fontsize=15, ha='right', va='center')
        
# Subplot 8
df8 = dataframes_fig3[7]
colors8 = colors_fig3[7]
ax8 = axes[2,1]
df8.plot(kind='area', ax=ax8, xlim=(1990, 2020), ylim=(0, 18), xlabel='',
         grid=True, color=colors8, linewidth=0, legend=False)
ax8.set_title('h) Urban and rural Food-eHANPP', fontsize=15)
ax8.set_ylabel('Gt dm/yr', fontsize=15)
ax8.set_xlabel('', fontsize=12)
ax8.tick_params(axis='x', labelsize=15)
ax8.tick_params(axis='y', labelsize=15)
ax8.set_xticks([1990, 2000, 2010, 2020])
ax8.set_yticks([0, 3, 6, 9, 12, 15, 18])
ax8.grid(color='black', linewidth=0.3, alpha=0.2)
groups8 = df8.columns
cumulative8 = df8.cumsum(axis=1)
for group in groups8:
    x_pos = 2020
    if x_pos in df8.index:
        y_pos = (cumulative8[group] - df8[group] / 2).loc[x_pos]
        ax8.text(x_pos, y_pos, group, fontsize=15, ha='right', va='center')        
        
# Subplot 9
df9 = dataframes_fig3[8]
colors9 = colors_fig3[8]
ax9 = axes[2,2]
df9.plot(kind='line', ax=ax9, xlim=(1990, 2020), ylim=(0, 4), xlabel='',
         grid=True, color=colors9, linewidth=1, legend=True)
ax9.set_title('i) Urban and rural Food-eHANPP per capita', fontsize=15)
ax9.legend(title='', loc = 'lower left', fontsize = 12)
ax9.set_ylabel('t dm/cap/yr', fontsize=15)
ax9.set_xlabel('', fontsize=15)
ax9.tick_params(axis='x', labelsize=15)
ax9.tick_params(axis='y', labelsize=15)
ax9.set_xticks([1990, 2000, 2010, 2020])
ax9.set_yticks([0,1,2,3,4])
ax9.grid(color='black', linewidth=0.3, alpha=0.2)    
# Set individual line widths
lines = ax9.get_lines()
line_widths = [4, 4, 1, 1, 4, 1, 1]
for line, lw in zip(lines, line_widths):
    line.set_linewidth(lw)    
# define lines for shading
x = df9.index
y1 = df9['rural-lower boundary']  # First line
y2 = df9['rural-upper boundary']  # Second line
y3 = df9['urban-lower boundary']  # First line
y4 = df9['urban-upper boundary']  # Second line
# Fill the area between the two lines
ax9.fill_between(x, y1, y2, color='#00B050', alpha=0.3)    
ax9.fill_between(x, y3, y4, color='#C9C9C9', alpha=0.3)        
plt.tight_layout()
plt.show()
plt.savefig(path +'/figure3.png', dpi=300, bbox_inches='tight')


###############################################################################
#PLOT: Figure 4abcd 

dataframes_fig4 = [df_food_reg_urbrur_cap_FeH_1990, df_food_reg_urbrur_cap_FeH_2019,
                   df_food_reg_urbrur_FeHint_cap_1990, df_food_reg_urbrur_FeHint_cap_2019,
                   df_food_reg_urbrur_cap_live_1990, df_food_reg_urbrur_cap_live_2019,
                   df_food_reg_urbrur_cap_plant_1990, df_food_reg_urbrur_cap_plant_2019]
colors_fig4 = ['#F4D03F','#2874A6','#A93226','#73C6B6','#595959']


fig, axes = plt.subplots(2, 2, figsize=(10, 10)) # Define each subplot individually
#Subplot 1
df1 = dataframes_fig4[0]
df2 = dataframes_fig4[1]
ax1 = axes[0,0]
df1.plot.scatter(
    x='urban_FeH_cap', y='rural_FeH_cap', xlim=(0,5),
    xticks=[0,1,2,3,4,5], ylim=(0,5), yticks=[0,1,2,3,4,5],
    xlabel='Urban Food-eHANPP in t dm/cap/yr\n', ylabel='Rural Food-eHANPP in t dm/cap/yr',
    legend=True, c=colors_fig4, s=80, ax=ax1)
ax1.scatter( # Plot the second set of points on the same axes
    df2['urban_FeH_cap'], df2['rural_FeH_cap'], 
    c=colors_fig4, s=80)
for i in range(len(df2)): # Add black arrows connecting the points from 1990 to 2019 for each region
    ax1.annotate(
        '', xy=(df2['urban_FeH_cap'].iloc[i], df2['rural_FeH_cap'].iloc[i]), 
        xytext=(df1['urban_FeH_cap'].iloc[i], df1['rural_FeH_cap'].iloc[i]),
        arrowprops=dict(
            arrowstyle="->,head_length=0.4,head_width=0.2", 
            color='black', 
            linewidth=1.5,
            shrinkA=0, shrinkB=0))
ax1.set_xlabel('Urban Food-eHANPP in t dm/cap/yr', fontsize=12)  
ax1.set_ylabel('Rural Food-eHANPP in t dm/cap/yr', fontsize=12)  
ax1.tick_params(axis='x', labelsize=12)  
ax1.tick_params(axis='y', labelsize=12)  
ax1.grid(color='gray', linewidth=0.5, alpha=0.2)
ax1.set_title('a) Urban and rural per capita Food-eHANPP')
ax1.plot([0, 5], [0, 5], linestyle='--', color='black')
legend_labels = df_food_reg_urbrur_cap_FeH_1990.index.unique()
legend_handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10, label=label) 
                  for color, label in zip(colors_fig4, legend_labels)]
ax1.legend(handles=legend_handles, fontsize=9.5, loc='upper left', frameon=False, bbox_to_anchor=(-0.03, 1), handletextpad=0.1)
textstr = 'arrow from 1990 to 2019' # Add a textbox in the bottom right corner
props = dict(boxstyle='round', facecolor='white', edgecolor='black', alpha=0.8)
ax1.text(0.95, 0.05, textstr, transform=ax1.transAxes, fontsize=8,
         verticalalignment='bottom', horizontalalignment='right', bbox=props)
xlim1 = ax1.get_xlim()
ylim1 = ax1.get_ylim()
aspect_ratio1 = (xlim1[1] - xlim1[0]) / (ylim1[1] - ylim1[0])
ax1.set_aspect(aspect_ratio1)

# Subplot 2
df3 = dataframes_fig4[2]
df4 = dataframes_fig4[3]
ax2 = axes[0,1]
df3.plot.scatter(
    x='urban_FeHint_cap', y='rural_FeHint_cap', xlim=(0,6),
    xticks=[0,1,2,3,4,5,6], ylim=(0,6), yticks=[0,1,2,3,4,5,6],
    xlabel='Urban Food-eHANPP intensity in g dm/kcal/yr', ylabel='Rural Food-eHANPP intensity in g dm/kcal/yr',
    legend=True, c=colors_fig4, s=80, ax=ax2)
ax2.scatter( # Plot the second set of points on the same axes
    df4['urban_FeHint_cap'], df4['rural_FeHint_cap'], 
    c=colors_fig4, s=80)
for i in range(len(df4)): # Add black arrows connecting the points from 1990 to 2019 for each region
    ax2.annotate(
        '', xy=(df4['urban_FeHint_cap'].iloc[i], df4['rural_FeHint_cap'].iloc[i]), 
        xytext=(df3['urban_FeHint_cap'].iloc[i], df3['rural_FeHint_cap'].iloc[i]),
        arrowprops=dict(
            arrowstyle="->,head_length=0.4,head_width=0.2", 
            color='black', 
            linewidth=1.5,
            shrinkA=0, shrinkB=0))
ax2.set_xlabel('Urban Food-eHANPP intensity in g dm/kcal/yr', fontsize=12)  
ax2.set_ylabel('Rural Food-eHANPP intensity in g dm/kcal/yr', fontsize=12)  
ax2.tick_params(axis='x', labelsize=12)  
ax2.tick_params(axis='y', labelsize=12)  
ax2.grid(color='gray', linewidth=0.5, alpha=0.2)
ax2.set_title('b) Urban and rural Food-eHANPP intensity')
ax2.plot([0, 6], [0, 6], linestyle='--', color='black')
legend_labels = df_food_reg_urbrur_cap_FeH_1990.index.unique()
legend_handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10, label=label) 
                  for color, label in zip(colors_fig4, legend_labels)]
ax2.legend(handles=legend_handles, fontsize=9.5, loc='upper left', frameon=False, bbox_to_anchor=(-0.03, 1), handletextpad=0.1)
textstr = 'arrow from 1990 to 2019' # Add a textbox in the bottom right corner
props = dict(boxstyle='round', facecolor='white', edgecolor='black', alpha=0.8)
ax2.text(0.95, 0.05, textstr, transform=ax2.transAxes, fontsize=8,
         verticalalignment='bottom', horizontalalignment='right', bbox=props)
xlim2 = ax2.get_xlim()
ylim2 = ax2.get_ylim()
aspect_ratio2 = (xlim2[1] - xlim2[0]) / (ylim2[1] - ylim2[0])
ax2.set_aspect(aspect_ratio2)

#Subplot 3 
df5 = dataframes_fig4[4]
df6 = dataframes_fig4[5]
ax3 = axes[1,0]
df5.plot.scatter(
    x='urban_live_cap', y='rural_live_cap', xlim=(0,1250),
    xticks=[0,250,500,750,1000,1250], ylim=(0,1250), yticks=[0,250,500,750,1000,1250],
    xlabel='Urban livestock products supply in kcal/cap/day', ylabel='Rural livestock products supply in kcal/cap/day',
    legend=True, c=colors_fig4, s=80, ax=ax3)
ax3.scatter( # Plot the second set of points on the same axes
    df6['urban_live_cap'], df6['rural_live_cap'], 
    c=colors_fig4, s=80)
for i in range(len(df6)): # Add black arrows connecting the points from 1990 to 2019 for each region
    ax3.annotate(
        '', xy=(df6['urban_live_cap'].iloc[i], df6['rural_live_cap'].iloc[i]), 
        xytext=(df5['urban_live_cap'].iloc[i], df5['rural_live_cap'].iloc[i]),
        arrowprops=dict(
            arrowstyle="->,head_length=0.4,head_width=0.2", 
            color='black', 
            linewidth=1.5,
            shrinkA=0, shrinkB=0))
ax3.set_xlabel('Urban livestock products supply in kcal/cap/day', fontsize=12)  
ax3.set_ylabel('Rural livestock products supply in kcal/cap/day', fontsize=12)  
ax3.tick_params(axis='x', labelsize=12)  
ax3.tick_params(axis='y', labelsize=12)  
ax3.grid(color='gray', linewidth=0.5, alpha=0.2)
ax3.set_title('c) Urban and rural per capita supply\nlivestock products')
ax3.plot([0, 2000], [0, 2000], linestyle='--', color='black')
legend_labels = df_food_reg_urbrur_cap_FeH_1990.index.unique()
legend_handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10, label=label) 
                  for color, label in zip(colors_fig4, legend_labels)]
ax3.legend(handles=legend_handles, fontsize=9.5, loc='upper left', frameon=False, bbox_to_anchor=(-0.03, 1), handletextpad=0.1)
textstr = 'arrow from 1990 to 2019' # Add a textbox in the bottom right corner
props = dict(boxstyle='round', facecolor='white', edgecolor='black', alpha=0.8)
ax3.text(0.95, 0.05, textstr, transform=ax3.transAxes, fontsize=8,
         verticalalignment='bottom', horizontalalignment='right', bbox=props)
xlim3 = ax3.get_xlim()
ylim3 = ax3.get_ylim()
aspect_ratio3 = (xlim3[1] - xlim3[0]) / (ylim3[1] - ylim3[0])
ax3.set_aspect(aspect_ratio3)

#Subplot 4
df7 = dataframes_fig4[6]
df8 = dataframes_fig4[7]
ax4 = axes[1,1]
df7.plot.scatter(
    x='urban_plant_cap', y='rural_plant_cap', xlim=(1000,3500),
    xticks=[1000,1500,2000,2500,3000,3500], ylim=(1000,3500), yticks=[1000,1500,2000,2500,3000,3500],
    xlabel='Urban plant supply in kcal/cap/yr', ylabel='Rural plant supply in kcal/cap/yr',
    legend=True, c=colors_fig4, s=80, ax=ax4)
ax4.scatter( # Plot the second set of points on the same axes
    df8['urban_plant_cap'], df8['rural_plant_cap'], 
    c=colors_fig4, s=80)
for i in range(len(df8)): # Add black arrows connecting the points from 1990 to 2019 for each region
    ax4.annotate(
        '', xy=(df8['urban_plant_cap'].iloc[i], df8['rural_plant_cap'].iloc[i]), 
        xytext=(df7['urban_plant_cap'].iloc[i], df7['rural_plant_cap'].iloc[i]),
        arrowprops=dict(
            arrowstyle="->,head_length=0.4,head_width=0.2", 
            color='black', 
            linewidth=1.5,
            shrinkA=0, shrinkB=0))
ax4.set_xlabel('Urban plant supply in kcal/cap/yr', fontsize=12)  
ax4.set_ylabel('Rural plant supply in kcal/cap/yr', fontsize=12)  
ax4.tick_params(axis='x', labelsize=12)  
ax4.tick_params(axis='y', labelsize=12)  
ax4.grid(color='gray', linewidth=0.5, alpha=0.2)
ax4.set_title('d) Urban and rural per capita supply\nplant products')
ax4.plot([0, 3500], [0, 3500], linestyle='--', color='black')
legend_labels = df_food_reg_urbrur_cap_FeH_1990.index.unique()
legend_handles = [plt.Line2D([0], [0], marker='o', color='w', markerfacecolor=color, markersize=10, label=label) 
                  for color, label in zip(colors_fig4, legend_labels)]
ax4.legend(handles=legend_handles, fontsize=9.5, loc='upper left', frameon=False, bbox_to_anchor=(-0.03, 1), handletextpad=0.1)
textstr = 'arrow from 1990 to 2019' # Add a textbox in the bottom right corner
props = dict(boxstyle='round', facecolor='white', edgecolor='black', alpha=0.8)
ax4.text(0.95, 0.05, textstr, transform=ax4.transAxes, fontsize=8,
         verticalalignment='bottom', horizontalalignment='right', bbox=props)
xlim4 = ax4.get_xlim()
ylim4 = ax4.get_ylim()
aspect_ratio4 = (xlim4[1] - xlim4[0]) / (ylim4[1] - ylim4[0])
ax4.set_aspect(aspect_ratio4)

plt.tight_layout()
plt.show()
plt.savefig(path +'/figure4.png', dpi=300, bbox_inches='tight')



###############################################################################
#SI figures

#Dataframe for SI Figure 5a: Urban/Rural Food-eHANPP per capita

# Base dataframe
df_food_reg_urbrur_cap_FeH_SI = df_food_regional.groupby(['income_group','Year']).sum().reset_index()
df_food_reg_urbrur_cap_FeH_SI = df_food_reg_urbrur_cap_FeH_SI[['income_group','Year','FeH_urban_median','FeH_rural_median']]

# Merge with population
df_food_reg_urbrur_cap_FeH_SI = df_food_reg_urbrur_cap_FeH_SI.merge(df_pop_reg, how='left', on=['income_group','Year'])

# Add global row per year
df_global_SI = df_food_reg_urbrur_cap_FeH_SI.groupby('Year').sum().reset_index()
df_global_SI['income_group'] = 'Global'

# Combine with regional data
df_food_reg_urbrur_cap_FeH_all_SI = pd.concat([df_food_reg_urbrur_cap_FeH_SI, df_global_SI], ignore_index=True)

# Compute per capita values
df_food_reg_urbrur_cap_FeH_all_SI['urban_FeH_cap'] = (df_food_reg_urbrur_cap_FeH_all_SI['FeH_urban_median'] / df_food_reg_urbrur_cap_FeH_all_SI['urban population'])
df_food_reg_urbrur_cap_FeH_all_SI['rural_FeH_cap'] = (df_food_reg_urbrur_cap_FeH_all_SI['FeH_rural_median'] / df_food_reg_urbrur_cap_FeH_all_SI['rural population'])

# Apply mapping and order
df_food_reg_urbrur_cap_FeH_all_SI['income_group'] = df_food_reg_urbrur_cap_FeH_all_SI['income_group'].replace(income_group_mapping)

df_food_reg_urbrur_cap_FeH_all_SI = df_food_reg_urbrur_cap_FeH_all_SI.set_index(['Year','income_group'])
df_food_reg_urbrur_cap_FeH_all_SI = df_food_reg_urbrur_cap_FeH_all_SI[['urban_FeH_cap','rural_FeH_cap']]

# Optional: sort income groups
df_food_reg_urbrur_cap_FeH_all_SI = (df_food_reg_urbrur_cap_FeH_all_SI.reindex(income_group_order, level=1).sort_index())

# Custom color mapping for income groups
income_groups_SI = ['Low-income','Lower-middle-income','Upper-middle-income','High-income','Global']
color_map = dict(zip(income_groups_SI, colors_fig4))
plt.figure(figsize=(12, 7))
for group in income_groups_SI:
    df_group = df_food_reg_urbrur_cap_FeH_all_SI.reset_index()
    df_group = df_group[df_group['income_group'] == group]
    # Urban: solid line
    plt.plot(df_group['Year'], df_group['urban_FeH_cap'],
             label=f"{group} - Urban",
             color=color_map[group], linestyle='-')
    # Rural: dashed line
    plt.plot(df_group['Year'], df_group['rural_FeH_cap'],
             label=f"{group} - Rural",
             color=color_map[group], linestyle='--')
plt.title("Urban vs. Rural Food-eHANPP per capita by income group (1990-2020)", fontsize = 14)
plt.ylabel("Food-eHANPP in t dm/cap/yr'", fontsize = 12)
plt.xlim(1990,2020)
plt.xticks(fontsize = 12)
plt.yticks(fontsize = 12)
plt.grid()
plt.legend(ncol=2, fontsize=9, frameon=False)  # 2-column legend for readability
plt.tight_layout()
plt.show()
plt.savefig(path +'/figureS5a.png', dpi=300, bbox_inches='tight')
###############################################################################
#Dataframe for Figure S5b: Urban/Rural Food-eHANPP intensity
# Base dataframe
df_food_reg_urbrur_FeHint_cap_SI = df_food_regional.groupby(['income_group','Year']).sum().reset_index()
df_food_reg_urbrur_FeHint_cap_SI = df_food_reg_urbrur_FeHint_cap_SI[[
    'income_group','Year','FeH_urban_median','FeH_rural_median',
    'kcal_urban_median','kcal_rural_median']]

# Add global row per year
df_global_SI = df_food_reg_urbrur_FeHint_cap_SI.groupby('Year').sum().reset_index()
df_global_SI['income_group'] = 'Global'
# Combine with regional data
df_food_reg_urbrur_FeHint_cap_all_SI = pd.concat([df_food_reg_urbrur_FeHint_cap_SI, df_global_SI], ignore_index=True)

# Compute per kcal values
df_food_reg_urbrur_FeHint_cap_all_SI['urban_FeHint_cap'] = (df_food_reg_urbrur_FeHint_cap_all_SI['FeH_urban_median'] / df_food_reg_urbrur_FeHint_cap_all_SI['kcal_urban_median'])
df_food_reg_urbrur_FeHint_cap_all_SI['rural_FeHint_cap'] = (df_food_reg_urbrur_FeHint_cap_all_SI['FeH_rural_median'] / df_food_reg_urbrur_FeHint_cap_all_SI['kcal_rural_median'])

# Convert from t/kcal to g/kcal
df_food_reg_urbrur_FeHint_cap_all_SI['urban_FeHint_cap'] *= 1000 * 1000
df_food_reg_urbrur_FeHint_cap_all_SI['rural_FeHint_cap'] *= 1000 * 1000

# Apply mapping
df_food_reg_urbrur_FeHint_cap_all_SI['income_group'] = df_food_reg_urbrur_FeHint_cap_all_SI['income_group'].replace(income_group_mapping)

# Set index for clean structure
df_food_reg_urbrur_FeHint_cap_all_SI = df_food_reg_urbrur_FeHint_cap_all_SI.set_index(['Year','income_group'])
df_food_reg_urbrur_FeHint_cap_all_SI = df_food_reg_urbrur_FeHint_cap_all_SI[['urban_FeHint_cap','rural_FeHint_cap']]

# Sort income groups for consistency
df_food_reg_urbrur_FeHint_cap_all_SI = (df_food_reg_urbrur_FeHint_cap_all_SI.reindex(income_group_order, level=1).sort_index())

plt.figure(figsize=(12, 7))
for group in income_groups_SI:
    df_group = df_food_reg_urbrur_FeHint_cap_all_SI.reset_index()
    df_group = df_group[df_group['income_group'] == group]
    # Urban: solid line
    plt.plot(df_group['Year'], df_group['urban_FeHint_cap'],
             label=f"{group} - Urban",
             color=color_map[group], linestyle='-')
    # Rural: dashed line
    plt.plot(df_group['Year'], df_group['rural_FeHint_cap'],
             label=f"{group} - Rural",
             color=color_map[group], linestyle='--')
plt.title("Urban vs. Rural Food-eHANPP intensity (1990-2020)", fontsize = 14)
plt.ylabel("Food-eHANPP intensity in  g dm/kcal/yr", fontsize = 12)
plt.xlim(1990,2020)
plt.xticks(fontsize = 12)
plt.yticks(fontsize = 12)
plt.grid()
plt.legend(ncol=2, fontsize=9, frameon=False)  # 2-column legend for readability
plt.tight_layout()
plt.show()
plt.savefig(path +'/figureS5b.png', dpi=300, bbox_inches='tight')
###############################################################################
#Dataframe for Figure S5c: Urban/Rural livestock supply
# Filter for livestock products
df_food_reg_urbrur_cap_live_SI = df_food_regional[['income_group','Year','food_group','kcal_urban_median','kcal_rural_median']]
df_food_reg_urbrur_cap_live_SI = df_food_reg_urbrur_cap_live_SI.loc[df_food_reg_urbrur_cap_live_SI['food_group'].isin(livestock_products)]

# Aggregate across livestock products
df_food_reg_urbrur_cap_live_SI = df_food_reg_urbrur_cap_live_SI.groupby(['income_group','Year']).sum().reset_index()
# Merge with population
df_food_reg_urbrur_cap_live_SI = df_food_reg_urbrur_cap_live_SI.merge(df_pop_reg, how='left', on=['income_group','Year'])
# Add global row per year
df_global_SI = df_food_reg_urbrur_cap_live_SI.groupby('Year').sum().reset_index()
df_global_SI['income_group'] = 'Global'
# Combine
df_food_reg_urbrur_cap_live_all_SI = pd.concat([df_food_reg_urbrur_cap_live_SI, df_global_SI], ignore_index=True)
# Compute per capita kcal/day
df_food_reg_urbrur_cap_live_all_SI['urban_live_cap'] = (df_food_reg_urbrur_cap_live_all_SI['kcal_urban_median'] / df_food_reg_urbrur_cap_live_all_SI['urban population']) / 365
df_food_reg_urbrur_cap_live_all_SI['rural_live_cap'] = (df_food_reg_urbrur_cap_live_all_SI['kcal_rural_median'] / df_food_reg_urbrur_cap_live_all_SI['rural population']) / 365
# Apply mapping
df_food_reg_urbrur_cap_live_all_SI['income_group'] = df_food_reg_urbrur_cap_live_all_SI['income_group'].replace(income_group_mapping)
# Set tidy index
df_food_reg_urbrur_cap_live_all_SI = df_food_reg_urbrur_cap_live_all_SI.set_index(['Year','income_group'])
df_food_reg_urbrur_cap_live_all_SI = df_food_reg_urbrur_cap_live_all_SI[['urban_live_cap','rural_live_cap']]
# Sort income groups
df_food_reg_urbrur_cap_live_all_SI = (df_food_reg_urbrur_cap_live_all_SI.reindex(income_group_order, level=1).sort_index())

plt.figure(figsize=(12, 7))
for group in income_groups_SI:
    df_group = df_food_reg_urbrur_cap_live_all_SI.reset_index()
    df_group = df_group[df_group['income_group'] == group]
    # Urban: solid line
    plt.plot(df_group['Year'], df_group['urban_live_cap'],
             label=f"{group} - Urban",
             color=color_map[group], linestyle='-')
    # Rural: dashed line
    plt.plot(df_group['Year'], df_group['rural_live_cap'],
             label=f"{group} - Rural",
             color=color_map[group], linestyle='--')
plt.title("Urban vs. Rural livestock supply (1990-2020)", fontsize = 14)
plt.ylabel("livestock products supply in kcal/cap/day", fontsize = 12)
plt.xlim(1990,2020)
plt.xticks(fontsize = 12)
plt.yticks(fontsize = 12)
plt.grid()
plt.legend(ncol=2, fontsize=9, frameon=False, loc='lower center', bbox_to_anchor=(0.22, 0.65))  # 2-column legend for readability
plt.tight_layout()
plt.show()
plt.savefig(path +'/figureS5c.png', dpi=300, bbox_inches='tight')


###############################################################################
#Dataframe for Figure S5d: Urban/rural plant-based supply
df_food_reg_urbrur_cap_plant_SI = df_food_regional[['income_group','Year','food_group','kcal_urban_median','kcal_rural_median']]
df_food_reg_urbrur_cap_plant_SI = df_food_reg_urbrur_cap_plant_SI.loc[~df_food_reg_urbrur_cap_plant_SI['food_group'].isin(livestock_products)]
# Aggregate across plant products
df_food_reg_urbrur_cap_plant_SI = df_food_reg_urbrur_cap_plant_SI.groupby(['income_group','Year']).sum().reset_index()
# Merge with population
df_food_reg_urbrur_cap_plant_SI = df_food_reg_urbrur_cap_plant_SI.merge(df_pop_reg, how='left', on=['income_group','Year'])
# Add global row per year
df_global_SI = df_food_reg_urbrur_cap_plant_SI.groupby('Year').sum().reset_index()
df_global_SI['income_group'] = 'Global'
# Combine
df_food_reg_urbrur_cap_plant_all_SI = pd.concat([df_food_reg_urbrur_cap_plant_SI, df_global_SI], ignore_index=True)
# Compute per capita kcal/day
df_food_reg_urbrur_cap_plant_all_SI['urban_plant_cap'] = (df_food_reg_urbrur_cap_plant_all_SI['kcal_urban_median'] / df_food_reg_urbrur_cap_plant_all_SI['urban population']) / 365
df_food_reg_urbrur_cap_plant_all_SI['rural_plant_cap'] = (df_food_reg_urbrur_cap_plant_all_SI['kcal_rural_median'] / df_food_reg_urbrur_cap_plant_all_SI['rural population']) / 365
# Apply mapping
df_food_reg_urbrur_cap_plant_all_SI['income_group'] = df_food_reg_urbrur_cap_plant_all_SI['income_group'].replace(income_group_mapping)
# Set tidy index
df_food_reg_urbrur_cap_plant_all_SI = df_food_reg_urbrur_cap_plant_all_SI.set_index(['Year','income_group'])
df_food_reg_urbrur_cap_plant_all_SI = df_food_reg_urbrur_cap_plant_all_SI[['urban_plant_cap','rural_plant_cap']]
# Sort income groups
df_food_reg_urbrur_cap_plant_all_SI = (df_food_reg_urbrur_cap_plant_all_SI.reindex(income_group_order, level=1).sort_index())

plt.figure(figsize=(12, 7))
for group in income_groups_SI:
    df_group = df_food_reg_urbrur_cap_plant_all_SI.reset_index()
    df_group = df_group[df_group['income_group'] == group]
    # Urban: solid line
    plt.plot(df_group['Year'], df_group['urban_plant_cap'],
             label=f"{group} - Urban",
             color=color_map[group], linestyle='-')
    # Rural: dashed line
    plt.plot(df_group['Year'], df_group['rural_plant_cap'],
             label=f"{group} - Rural",
             color=color_map[group], linestyle='--')
plt.title("Urban vs. Rural plant supply (1990-2020)", fontsize = 14)
plt.ylabel("plant products supply in kcal/cap/day", fontsize = 12)
plt.xlim(1990,2020)
plt.xticks(fontsize = 12)
plt.yticks(fontsize = 12)
plt.grid()
plt.legend(ncol=2, fontsize=9, frameon=False, loc='lower center', bbox_to_anchor=(0.22, 0.3))  # 2-column legend for readability
plt.tight_layout()
plt.show()
plt.savefig(path +'/figureS5d.png', dpi=300, bbox_inches='tight')











