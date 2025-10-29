# -*- coding: utf-8 -*-

"""
Title: GDD Data collection lower uncertainty interval (2.5°) for mean intake
Author: Lisa Kaufmann
Affiliation: BOKU University, Institute of Social Ecology, Department of Economics and Social Sciences, Schottenfeldgasse 29, 1070 Vienna, Austria
Contact: lisa.kaufmann@boku.ac.at
Date: October 27, 2025
Version: 1.0
License: XX
Repository: https://github.com/lisakaufmannsec/Food-eHANPP
Manuscript: Income level and urbanization shape food-related pressures on ecosystems "
Description: Code extracts the required GDD data from csv-Files

"""


import pandas as pd

path = r'' # place of extracted GDD files

#--------------Fruits and Fruit juice (v01 und v01_v16)------------------------

#load data frame
data_v01 = pd.read_csv(path + '\v01_cnty.csv')
df_v01 = pd.DataFrame(data_v01, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v01 = df_v01.loc[df_v01['age'] == 999]
df_v01 = df_v01.loc[df_v01['female'] == 999]
df_v01 = df_v01.loc[df_v01['edu'] == 999]
df_v01 = df_v01.loc[df_v01['urban'] != 999]
df_v01 = df_v01.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v01 = df_v01.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v01.reset_index(inplace=True)
#rename columns
df_v01.columns = ['Country','Year','rural', 'urban']


#load data frame
data_v16 = pd.read_csv(path + '/v16_cnty.csv')
df_v16 = pd.DataFrame(data_v16, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v16 = df_v16.loc[df_v16['age'] == 999]
df_v16 = df_v16.loc[df_v16['female'] == 999]
df_v16 = df_v16.loc[df_v16['edu'] == 999]
df_v16 = df_v16.loc[df_v16['urban'] != 999]
df_v16 = df_v16.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v16 = df_v16.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v16.reset_index(inplace=True)
#rename columns
df_v16.columns = ['Country','Year','rural', 'urban']

df_v01_v16 = df_v01.merge(df_v16, on=['Country','Year'], how = 'outer', suffixes=('_v01', '_v16'))

df_v01_v16['rural_v01_v16'] = df_v01_v16['rural_v01'] + df_v01_v16['rural_v16'] 
df_v01_v16['urban_v01_v16'] = df_v01_v16['urban_v01'] + df_v01_v16['urban_v16'] 
df_v01_v16 = df_v01_v16.drop(['rural_v01','urban_v01','rural_v16','urban_v16'], axis=1)

df_v01_v16['GDD_item_code'] = 'v01_v16'
df_v01_v16 = df_v01_v16.rename(columns={'rural_v01_v16': 'GDD_rural','urban_v01_v16': 'GDD_urban'})
#change order of colomns
df_v01_v16 = df_v01_v16[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#--------------------Non-starchy vegetables (v02)------------------------------
#load data frame
data_v02 = pd.read_csv(path + '/v02_cnty.csv')
df_v02 = pd.DataFrame(data_v02, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v02 = df_v02.loc[df_v02['age'] == 999]
df_v02 = df_v02.loc[df_v02['female'] == 999]
df_v02 = df_v02.loc[df_v02['edu'] == 999]
df_v02 = df_v02.loc[df_v02['urban'] != 999]
df_v02 = df_v02.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v02 = df_v02.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v02.reset_index(inplace=True)
#rename columns
df_v02.columns = ['Country','Year','rural', 'urban']

df_v02['GDD_item_code'] = 'v02'
df_v02 = df_v02.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v02 = df_v02[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#------------------------Potatoes (v03)----------------------------------------
#load data frame
data_v03 = pd.read_csv(path + '/v03_cnty.csv')
df_v03 = pd.DataFrame(data_v03, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v03 = df_v03.loc[df_v03['age'] == 999]
df_v03 = df_v03.loc[df_v03['female'] == 999]
df_v03 = df_v03.loc[df_v03['edu'] == 999]
df_v03 = df_v03.loc[df_v03['urban'] != 999]
df_v03 = df_v03.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v03 = df_v03.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v03.reset_index(inplace=True)
#rename columns
df_v03.columns = ['Country','Year','rural', 'urban']

df_v03['GDD_item_code'] = 'v03'
df_v03 = df_v03.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v03 = df_v03[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#--------------------other starchy vegetables (v04)----------------------------
#load data frame
data_v04 = pd.read_csv(path + '/v04_cnty.csv')
df_v04 = pd.DataFrame(data_v04, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v04 = df_v04.loc[df_v04['age'] == 999]
df_v04 = df_v04.loc[df_v04['female'] == 999]
df_v04 = df_v04.loc[df_v04['edu'] == 999]
df_v04 = df_v04.loc[df_v04['urban'] != 999]
df_v04 = df_v04.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v04 = df_v04.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v04.reset_index(inplace=True)
#rename columns
df_v04.columns = ['Country','Year','rural', 'urban']

df_v04['GDD_item_code'] = 'v04'
df_v04 = df_v04.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v04 = df_v04[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#--------------------Beans and legumes (v05)-----------------------------------
#load data frame
data_v05 = pd.read_csv(path + '/v05_cnty.csv')
df_v05 = pd.DataFrame(data_v05, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v05 = df_v05.loc[df_v05['age'] == 999]
df_v05 = df_v05.loc[df_v05['female'] == 999]
df_v05 = df_v05.loc[df_v05['edu'] == 999]
df_v05 = df_v05.loc[df_v05['urban'] != 999]
df_v05 = df_v05.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v05 = df_v05.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v05.reset_index(inplace=True)
#rename columns
df_v05.columns = ['Country','Year','rural', 'urban']

df_v05['GDD_item_code'] = 'v05'
df_v05 = df_v05.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v05 = df_v05[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]


#--------------------Nuts and seeds (v06)--------------------------------------
#load data frame
data_v06 = pd.read_csv(path + '/v06_cnty.csv')
df_v06 = pd.DataFrame(data_v06, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v06 = df_v06.loc[df_v06['age'] == 999]
df_v06 = df_v06.loc[df_v06['female'] == 999]
df_v06 = df_v06.loc[df_v06['edu'] == 999]
df_v06 = df_v06.loc[df_v06['urban'] != 999]
df_v06 = df_v06.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v06 = df_v06.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v06.reset_index(inplace=True)
#rename columns
df_v06.columns = ['Country','Year','rural', 'urban']

df_v06['GDD_item_code'] = 'v06'
df_v06 = df_v06.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v06 = df_v06[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]


#--------------------Refined and whole grains (v07_v08)------------------------
#load data frame
data_v07 = pd.read_csv(path + '/v07_cnty.csv')
df_v07 = pd.DataFrame(data_v07, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v07 = df_v07.loc[df_v07['age'] == 999]
df_v07 = df_v07.loc[df_v07['female'] == 999]
df_v07 = df_v07.loc[df_v07['edu'] == 999]
df_v07 = df_v07.loc[df_v07['urban'] != 999]
df_v07 = df_v07.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v07 = df_v07.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v07.reset_index(inplace=True)
#rename columns
df_v07.columns = ['Country','Year','rural', 'urban']


#load data frame
data_v08 = pd.read_csv(path + '/v08_cnty.csv')
df_v08 = pd.DataFrame(data_v08, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v08 = df_v08.loc[df_v08['age'] == 999]
df_v08 = df_v08.loc[df_v08['female'] == 999]
df_v08 = df_v08.loc[df_v08['edu'] == 999]
df_v08 = df_v08.loc[df_v08['urban'] != 999]
df_v08 = df_v08.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v08 = df_v08.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v08.reset_index(inplace=True)
#rename columns
df_v08.columns = ['Country','Year','rural', 'urban']

df_v07_v08 = df_v07.merge(df_v08, on=['Country','Year'], how = 'outer', suffixes=('_v07', '_v08'))

df_v07_v08['rural_v07_v08'] = df_v07_v08['rural_v07'] + df_v07_v08['rural_v08'] 
df_v07_v08['urban_v07_v08'] = df_v07_v08['urban_v07'] + df_v07_v08['urban_v08'] 
df_v07_v08 = df_v07_v08.drop(['rural_v07','urban_v07','rural_v08','urban_v08'], axis=1)

df_v07_v08['GDD_item_code'] = 'v07_v08'
df_v07_v08 = df_v07_v08.rename(columns={'rural_v07_v08': 'GDD_rural','urban_v07_v08': 'GDD_urban'})
#change order of colomns
df_v07_v08 = df_v07_v08[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]


#--------------------Meats (unprocessed and processed) v09_v10)----------------
#load data frame
data_v09 = pd.read_csv(path + '/v09_cnty.csv')
df_v09 = pd.DataFrame(data_v09, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v09 = df_v09.loc[df_v09['age'] == 999]
df_v09 = df_v09.loc[df_v09['female'] == 999]
df_v09 = df_v09.loc[df_v09['edu'] == 999]
df_v09 = df_v09.loc[df_v09['urban'] != 999]
df_v09 = df_v09.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v09 = df_v09.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v09.reset_index(inplace=True)
#rename columns
df_v09.columns = ['Country','Year','rural', 'urban']

data_v10 = pd.read_csv(path + '/v10_cnty.csv')
df_v10 = pd.DataFrame(data_v10, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v10 = df_v10.loc[df_v10['age'] == 999]
df_v10 = df_v10.loc[df_v10['female'] == 999]
df_v10 = df_v10.loc[df_v10['edu'] == 999]
df_v10 = df_v10.loc[df_v10['urban'] != 999]
df_v10 = df_v10.loc[df_v10['year'] != 2020]
df_v10 = df_v10.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v10 = df_v10.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v10.reset_index(inplace=True)
#rename columns
df_v10.columns = ['Country','Year','rural', 'urban']

df_v09_v10 = df_v09.merge(df_v10, on=['Country','Year'], how = 'outer', suffixes=('_v09', '_v10'))

df_v09_v10['rural_v09_v10'] = df_v09_v10['rural_v09'] + df_v09_v10['rural_v10'] 
df_v09_v10['urban_v09_v10'] = df_v09_v10['urban_v09'] + df_v09_v10['urban_v10'] 
df_v09_v10 = df_v09_v10.drop(['rural_v09','urban_v09','rural_v10','urban_v10'], axis=1)

df_v09_v10['GDD_item_code'] = 'v09_v10'
df_v09_v10 = df_v09_v10.rename(columns={'rural_v09_v10': 'GDD_rural','urban_v09_v10': 'GDD_urban'})
#change order of colomns
df_v09_v10 = df_v09_v10[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]


#--------------------Eggs (v12)------------------------------------------------
data_v12 = pd.read_csv(path + '/v12_cnty.csv')
df_v12 = pd.DataFrame(data_v12, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v12 = df_v12.loc[df_v12['age'] == 999]
df_v12 = df_v12.loc[df_v12['female'] == 999]
df_v12 = df_v12.loc[df_v12['edu'] == 999]
df_v12 = df_v12.loc[df_v12['urban'] != 999]
df_v12 = df_v12.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v12 = df_v12.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v12.reset_index(inplace=True)
#rename columns
df_v12.columns = ['Country','Year','rural', 'urban']

df_v12['GDD_item_code'] = 'v12'
df_v12 = df_v12.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v12 = df_v12[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#--------------------Coffee...................---------------------------------
data_v17 = pd.read_csv(path + '\Data_extract/v17_cnty.csv')
df_v17 = pd.DataFrame(data_v17, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v17 = df_v17.loc[df_v17['age'] == 999]
df_v17 = df_v17.loc[df_v17['female'] == 999]
df_v17 = df_v17.loc[df_v17['edu'] == 999]
df_v17 = df_v17.loc[df_v17['urban'] != 999]
df_v17 = df_v17.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v17 = df_v17.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v17.reset_index(inplace=True)
#rename columns
df_v17.columns = ['Country','Year','rural', 'urban']

df_v17['GDD_item_code'] = 'v17'
df_v17 = df_v17.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v17 = df_v17[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#--------------------Tea---...................---------------------------------
data_v18 = pd.read_csv(path + '/v18_cnty.csv')
df_v18 = pd.DataFrame(data_v18, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v18 = df_v18.loc[df_v18['age'] == 999]
df_v18 = df_v18.loc[df_v18['female'] == 999]
df_v18 = df_v18.loc[df_v18['edu'] == 999]
df_v18 = df_v18.loc[df_v18['urban'] != 999]
df_v18 = df_v18.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v18 = df_v18.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v18.reset_index(inplace=True)
#rename columns
df_v18.columns = ['Country','Year','rural', 'urban']

df_v18['GDD_item_code'] = 'v18'
df_v18 = df_v18.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v18 = df_v18[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#--------------------Saturated fat---------------------------------------------
data_v27 = pd.read_csv(path + '/v27_cnty.csv')
df_v27 = pd.DataFrame(data_v27, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v27 = df_v27.loc[df_v27['age'] == 999]
df_v27 = df_v27.loc[df_v27['female'] == 999]
df_v27 = df_v27.loc[df_v27['edu'] == 999]
df_v27 = df_v27.loc[df_v27['urban'] != 999]
df_v27 = df_v27.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v27 = df_v27.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v27.reset_index(inplace=True)
#rename columns
df_v27.columns = ['Country','Year','rural', 'urban']

df_v27['GDD_item_code'] = 'v27'
df_v27 = df_v27.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v27 = df_v27[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#--------------------Monounsaturated fatty acids-------------------------------
data_v28 = pd.read_csv(path + '/v28_cnty.csv')
df_v28 = pd.DataFrame(data_v28, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v28 = df_v28.loc[df_v28['age'] == 999]
df_v28 = df_v28.loc[df_v28['female'] == 999]
df_v28 = df_v28.loc[df_v28['edu'] == 999]
df_v28 = df_v28.loc[df_v28['urban'] != 999]
df_v28 = df_v28.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v28 = df_v28.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v28.reset_index(inplace=True)
#rename columns
df_v28.columns = ['Country','Year','rural', 'urban']

df_v28['GDD_item_code'] = 'v28'
df_v28 = df_v28.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v28 = df_v28[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#------------------------------Total omega-6 fat-------------------------------
data_v29 = pd.read_csv(path + '/v29_cnty.csv')
df_v29 = pd.DataFrame(data_v29, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v29 = df_v29.loc[df_v29['age'] == 999]
df_v29 = df_v29.loc[df_v29['female'] == 999]
df_v29 = df_v29.loc[df_v29['edu'] == 999]
df_v29 = df_v29.loc[df_v29['urban'] != 999]
df_v29 = df_v29.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v29 = df_v29.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v29.reset_index(inplace=True)
#rename columns
df_v29.columns = ['Country','Year','rural', 'urban']

df_v29['GDD_item_code'] = 'v29'
df_v29 = df_v29.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v29 = df_v29[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]



#--------------------Plant omega 3 fat-----------------------------------------
data_v31 = pd.read_csv(path + '/v31_cnty.csv')
df_v31 = pd.DataFrame(data_v31, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v31 = df_v31.loc[df_v31['age'] == 999]
df_v31 = df_v31.loc[df_v31['female'] == 999]
df_v31 = df_v31.loc[df_v31['edu'] == 999]
df_v31 = df_v31.loc[df_v31['urban'] != 999]
df_v31 = df_v31.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v31 = df_v31.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v31.reset_index(inplace=True)
#rename columns
df_v31.columns = ['Country','Year','rural', 'urban']

df_v31['GDD_item_code'] = 'v31'
df_v31 = df_v31.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v31 = df_v31[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]


#--------------------added suggar (v35)----------------------------------------
data_v35 = pd.read_csv(path + '/v35_cnty.csv')
df_v35 = pd.DataFrame(data_v35, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v35 = df_v35.loc[df_v35['age'] == 999]
df_v35 = df_v35.loc[df_v35['female'] == 999]
df_v35 = df_v35.loc[df_v35['edu'] == 999]
df_v35 = df_v35.loc[df_v35['urban'] != 999]
df_v35 = df_v35.loc[df_v35['year'] != 2020]
df_v35 = df_v35.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v35 = df_v35.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v35.reset_index(inplace=True)
#rename columns
df_v35.columns = ['Country','Year','rural', 'urban']

df_v35['GDD_item_code'] = 'v35'
df_v35 = df_v35.rename(columns={'rural': 'GDD_rural','urban': 'GDD_urban'})
#change order of colomns
df_v35 = df_v35[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

#--------------------Milk and products (v13, v14, v57)-------------------------

data_v13 = pd.read_csv(path + '/v13_cnty.csv')
df_v13 = pd.DataFrame(data_v13, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v13 = df_v13.loc[df_v13['age'] == 999]
df_v13 = df_v13.loc[df_v13['female'] == 999]
df_v13 = df_v13.loc[df_v13['edu'] == 999]
df_v13 = df_v13.loc[df_v13['urban'] != 999]
df_v13 = df_v13.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v13 = df_v13.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v13.reset_index(inplace=True)
#rename columns
df_v13.columns = ['Country','Year','rural', 'urban']

data_v14 = pd.read_csv(path + '/v14_cnty.csv')
df_v14 = pd.DataFrame(data_v14, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v14 = df_v14.loc[df_v14['age'] == 999]
df_v14 = df_v14.loc[df_v14['female'] == 999]
df_v14 = df_v14.loc[df_v14['edu'] == 999]
df_v14 = df_v14.loc[df_v14['urban'] != 999]
df_v14 = df_v14.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v14 = df_v14.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v14.reset_index(inplace=True)
#rename columns
df_v14.columns = ['Country','Year','rural', 'urban']

data_v57 = pd.read_csv(path + '/v57_cnty.csv')
df_v57 = pd.DataFrame(data_v57, columns= ['iso3','age','female','urban','edu','year','lowerci_95'])
#delete unwanted rows (age, female, edu)
df_v57 = df_v57.loc[df_v57['age'] == 999]
df_v57 = df_v57.loc[df_v57['female'] == 999]
df_v57 = df_v57.loc[df_v57['edu'] == 999]
df_v57 = df_v57.loc[df_v57['urban'] != 999]
df_v57 = df_v57.drop(['age','female','edu'], axis=1)
#transpose urbanization values
df_v57 = df_v57.pivot(index= ['iso3','year'], columns='urban', values='lowerci_95')
df_v57.reset_index(inplace=True)
#rename columns
df_v57.columns = ['Country','Year','rural', 'urban']

df_v13_v14_v57 = df_v13.merge(df_v13, on=['Country','Year'], how = 'outer', suffixes=('_v13', '_v14'))
df_v13_v14_v57 = df_v13_v14_v57.merge(df_v57, on=['Country','Year'], how = 'outer', suffixes=('','_v57'))
df_v13_v14_v57 = df_v13_v14_v57.rename(columns={'urban': 'urban_v57','rural': 'rural_v57'})

df_v13_v14_v57['rural_v13_v14_v57'] = df_v13_v14_v57['rural_v13'] + df_v13_v14_v57['rural_v14'] + df_v13_v14_v57['rural_v57']
df_v13_v14_v57['urban_v13_v14_v57'] = df_v13_v14_v57['urban_v13'] + df_v13_v14_v57['urban_v14'] + df_v13_v14_v57['urban_v57']

df_v13_v14_v57 = df_v13_v14_v57.drop(['rural_v13','urban_v13','rural_v14','urban_v14','rural_v57','urban_v57'], axis=1)
df_v13_v14_v57['GDD_item_code'] = 'v13_v14_v57'
df_v13_v14_v57 = df_v13_v14_v57.rename(columns={'rural_v13_v14_v57': 'GDD_rural','urban_v13_v14_v57': 'GDD_urban'})
#change order of colomns
df_v13_v14_v57 = df_v13_v14_v57[['Country','Year','GDD_item_code','GDD_urban','GDD_rural']]

###############################################################################

#Concat alle Items

df_all_items = pd.concat([df_v01_v16,df_v02,df_v03,df_v04,df_v05,df_v06,df_v07_v08,df_v09_v10,df_v12,df_v13_v14_v57,df_v17,df_v18,df_v27,df_v28,df_v29,df_v31,df_v35])


###############################################################################
#interpolate between years and add 2019 and 2020 with values from 2018

# Create a DataFrame with all years, countries, and items combinations
all_combinations = pd.DataFrame([(year, country, item) for year in range(df_all_items['Year'].min(), df_all_items['Year'].max() + 1)
                                  for country in df_all_items['Country'].unique()
                                  for item in df_all_items['GDD_item_code'].unique()],
                                 columns=['Year', 'Country', 'GDD_item_code'])

# Merge the existing DataFrame with the DataFrame containing all combinations
df_result = pd.merge(all_combinations, df_all_items, on=['Year', 'Country', 'GDD_item_code'], how='left')

# Interpolate missing values to fill NaNs
df_result['GDD_urban'] = df_result.groupby(['Country', 'GDD_item_code'])['GDD_urban'].transform(lambda x: x.interpolate())
df_result['GDD_rural'] = df_result.groupby(['Country', 'GDD_item_code'])['GDD_rural'].transform(lambda x: x.interpolate())

# Add rows for missing years (2019 and 2020) by copying values from 2018
missing_years = [2019, 2020]
for year in missing_years:
    df_missing = df_result[df_result['Year'] == 2018].copy()
    df_missing['Year'] = year
    df_result = pd.concat([df_result, df_missing], ignore_index=True)

# Sort the DataFrame
df_result.sort_values(by=['Country', 'GDD_item_code', 'Year'], inplace=True)

###############################################################################




df_result.to_csv('GDD_data_collection_lowerci_95.csv')















