# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.4
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Logan County Parcel Data

# %%
import os
import requests
import pyogrio
import re
import zipfile
import io
import geopandas as gpd
import pandas as pd
import xml.etree.ElementTree as ET
import random
import itables
import plotnine
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %% [markdown]
# # Get jurisdiction boundaries

# %%
STANDARD_GEO_VINTAGE = 2023
JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH = "../morpc-censustiger-standardize/output_data/morpc-standardgeos-census-{}.gpkg".format(STANDARD_GEO_VINTAGE)
JURISDICTIONS_PARTS_FEATURECLASS_LAYER = "JURIS-COUNTY"
print("Data: {0}, layer={1}".format(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, JURISDICTIONS_PARTS_FEATURECLASS_LAYER))

# %%
INPUT_DIR = "./input_data"

inputDir = os.path.normpath(INPUT_DIR)
if not os.path.exists(inputDir):
    os.makedirs(inputDir)

jurisdictionsPartsRaw = morpc.load_spatial_data(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, layerName=JURISDICTIONS_PARTS_FEATURECLASS_LAYER, archiveDir=inputDir)

# %% [markdown]
# # Get parcels

# %%
parcel_url = 'https://services9.arcgis.com/mFxO7gBbusFBQ5o9/ArcGIS/rest/services/Logan_County_Parcels/FeatureServer/12'

# %%
parcels = morpcParcels.gdf_from_services(parcel_url)

# %%
parcels = parcels[['Parcel_N_1', 'Land_Use_C', 'Acres', 'geometry']]

# %% [markdown]
# # Get cama

# %%
cama_url = 'https://realestate.co.logan.oh.us/api/Document/PublicRecordsExcel.zip'

# %%
morpcParcels.download_and_unzip_archive(url=os.path.dirname(cama_url), filename=os.path.basename(cama_url), temp_dir='./logan_data/cama/', keep_zip=True)

# %%
# Export all columns samples
#all_columns = []
#for file in os.listdir('./logan_data/cama/'):
#    if file.endswith('.xlsx'):
#        table = pd.read_excel(os.path.join('./logan_data/cama/', file))
#        table.columns = [f"{x}_{file}" for x in table.columns.values]
#        all_columns.append(morpcParcels.sample_columns_from_df(table))
#all_columns = pd.concat(all_columns)
#all_columns.to_csv('./logan_data/all_cama_columns.csv')

# %%
build = morpcParcels.extract_fields_from_cama('./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Building.xlsx', columns=['Parcel Number U', 'Card', 'Year Built', 'Year Effective'])

# %%
dwell = morpcParcels.extract_fields_from_cama('./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Dwelling.xlsx', columns=['Parcel Number U', 'Card', 'Year Built', 'Year Effective'])

# %%
cama = pd.concat([build, dwell])

# %%
cama = cama.groupby('Parcel Number U').agg({'Year Effective':'max'}).reset_index()

# %% [markdown]
# # Get addresses and calculate units

# %%
addr_url = 'https://services9.arcgis.com/mFxO7gBbusFBQ5o9/arcgis/rest/services/Logan_County_Addresses/FeatureServer/0'

# %%
addr = morpcParcels.gdf_from_services(addr_url)

# %%
units = parcels.to_crs('epsg:6549').sjoin(addr).groupby('Parcel_N_1').agg({'LSN':'count'}).sort_values('LSN', ascending=False).rename(columns = {'LSN':'units'}).reset_index()

# %% [markdown]
# # Join parcels, units, and cama

# %%
parcels = parcels.set_index('Parcel_N_1').join(units.set_index('Parcel_N_1')).reset_index()

# %%
parcels = parcels.loc[parcels['Parcel_N_1']!=" "]

# %%
parcels['Parcel_N_1'] = [str(x) for x in parcels['Parcel_N_1']]

# %%
cama['Parcel Number U'] = [str(x) for x in cama['Parcel Number U']]

# %%
parcels = parcels.set_index('Parcel_N_1').join(cama.set_index('Parcel Number U'))

# %% [markdown]
# # Calculate geometries and points

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry')
parcels['geometry'] = parcels['geometry'].centroid
parcels = parcels.loc[~parcels['geometry'].isna()]
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]


# %%
def get_housing_units_field(table, acres_name, luc_name):
    table[acres_name] = [pd.to_numeric(x) for x in table[acres_name]]
    table[luc_name] = [str(x) for x in table[luc_name]]

    table.loc[(table[acres_name] > .75 ) & (table[luc_name].str.startswith('51')), 'housing_unit_type'] = "SF-LL"
    table.loc[(table[acres_name] <= .75) & (table[luc_name].str.startswith('51')), 'housing_unit_type'] = "SF-SL"
    table.loc[table[luc_name].str.startswith(('52', '53', '54', '55')), 'housing_unit_type'] = "SF-A"
    table.loc[table[luc_name].str.startswith('4'), 'housing_unit_type'] = "MF"
    return(table)


# %%
parcels = get_housing_units_field(parcels, 'Acres', 'Land_Use_C')

# %%
parcels.crs

# %%
parcels

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Logan'].to_crs('NAD83'), fill="None", color='black')
    + plotnine.geom_jitter(parcels, plotnine.aes(x='x', y='y', size = 'units', fill = 'housing_unit_type'), color="None")
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
   + plotnine.scale_size_radius(range=(.2,10), breaks = (1, 10, 50, 100))
     + plotnine.guides(size=plotnine.guide_legend(override_aes={'color':'black'}))
)

# %%
