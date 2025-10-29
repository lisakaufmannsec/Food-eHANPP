# Food-eHANPP
This repository contains data and codes for the calculation of urban and rural Food-eHANPP.

READ ME:

This repository contains data and code for the calculation of urban and rural Food-eHANPP between 1990 and 2020 for 191 countries. The methods and results are presented in the manuscript “Income level and urbanization shape food-related pressures on ecosystems” (currently under review). The underlying product-level HANPP dataset is described in a ‘Data in Brief’ (https://doi.org/10.1016/j.dib.2023.109725). Updates to the dataset are described in the above-mentioned manuscript and the code and the resulting dataset available on Zenodo (https://zenodo.org/records/17467782). The provided code in this repository explicitly refers to the differentiation between urban and rural food supply and Food-eHANPP, which is the main research presented in the manuscript. The programming language is Python (3.12.9). Required packages are dask, pandas, numpy and matplotlib. 

For the reproduction of results, the Global Dietary Database (GDD; Zip-File) must be downloaded (requires a login on https://globaldietarydatabase.org/); the country-level estimates extracted and the relevant data selected by running the three “GDD_data_collection” skripts. The uploaded code uses randomly generated data of urban and rural dietary intake that replaces the actual GGD urban and rural dietary intake data. 

Instructions: 

#1 Download Data 

a) Product-level HANPP dataset including end-use: embodied_HANPP_all_uses_incl_ap_all_cl_gl_resid_infra_by_animal_products_zenodo.csv (https://zenodo.org/records/17467782/files/embodied_HANPP_all_uses_incl_ap_all_cl_gl_resid_infra_by_animal_products_zenodo.csv?download=1)
  The updated code is also provided on Zenodo: https://zenodo.org/records/17467782

b) Food supply data: food_supply.csv: extracted data from the ‘Metabalances’
    Stored as Zipfile on Github (https://github.com/lisakaufmannsec/Food-eHANPP/blob/main/food_supply.zip)
    Reconstruction of FAOSTAT food supply balances aligning old (prior to 2010) and old (2010 and later) methodology. The code for the construction of ‘Metabalances’ is part of the product-level HANPP data incl. end-uses           provided on Zenodo: https://zenodo.org/records/17467782

#2 Download and run the code that performs calculations and plots figures

This is the main calculation code. Inputs are:

•	Product-level HANPP incl. end-use dataset (see above)

•	Food supply (see above)

•	Three random GDD data collections representing the median (=here: mean), lower uncertainty interval (2.5°) for mean intake and the upper uncertainty interval (97.5°) for mean intake (replacing original data

•	Five sheets stored in look_up.xlsx containing country groups, food groups, urban and total population, dry matter content and calorie content

While the product-level HANPP dataset (Zenodo) and food supply (Github) have to be downloaded and the path changed accordingly, the remaining inputs are linked to Github and do not require any changes in the code. 

Figures will be saved in the same path (folder) as where the data downloads are located. 

