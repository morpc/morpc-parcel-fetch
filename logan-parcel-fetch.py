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
INPUT_DIR = "./input_data"
inputDir = os.path.normpath(INPUT_DIR)
if not os.path.exists(inputDir):
    os.makedirs(inputDir)
jurisdictionsPartsRaw = morpc.load_spatial_data(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, layerName=JURISDICTIONS_PARTS_FEATURECLASS_LAYER, archiveDir=inputDir)

# %% [markdown]
# # Archive input data

# %%
parcel_url = 'https://services9.arcgis.com/mFxO7gBbusFBQ5o9/ArcGIS/rest/services/Logan_County_Parcels/FeatureServer/12'
parcels = morpcParcels.gdf_from_services(parcel_url)
if not os.path.exists('./input_data/logan_data/addr/'):
    os.makedirs('./input_data/logan_data/addr/')
parcels.to_file("./input_data/logan_data/addr/logan_parcels.shp")

# %%
addr_url = 'https://services9.arcgis.com/mFxO7gBbusFBQ5o9/arcgis/rest/services/Logan_County_Addresses/FeatureServer/0'
parcels = morpcParcels.gdf_from_services(parcel_url)
if not os.path.exists('./input_data/logan_data/addr/'):
    os.makedirs('./input_data/logan_data/addr/')
parcels.to_file("./input_data/logan_data/addr/logan_parcels.shp")

# %%
cama_url = 'https://realestate.co.logan.oh.us/api/Document/PublicRecordsExcel.zip'
morpcParcels.download_and_unzip_archive(url=os.path.dirname(cama_url), filename=os.path.basename(cama_url), temp_dir='./logan_data/cama/', keep_zip=True)

# %% [markdown]
# # Load data

# %%
parcels = parcels[['Parcel_N_1', 'Land_Use_C', 'Acres', 'geometry']]

# %%
build = morpcParcels.extract_fields_from_cama('./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Building.xlsx', columns=['Parcel Number U', 'Card', 'Year Built', 'Year Effective'])

# %%
dwell = morpcParcels.extract_fields_from_cama('./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Dwelling.xlsx', columns=['Parcel Number U', 'Card', 'Year Built', 'Year Effective'])

# %%
cama = pd.concat([build, dwell])

# %%
cama = cama.groupby('Parcel Number U').agg({'Year Effective':'max'}).reset_index()

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
parcels = morpcParcels.get_housing_unit_type_field(parcels, 'Acres', 'Land_Use_C')

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
