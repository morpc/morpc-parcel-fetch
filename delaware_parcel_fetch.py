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
# # Delaware County Parcels

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
from IPython.display import clear_output

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %% [markdown]
# # Archive Data

# %%
addr_url = "https://services2.arcgis.com//ziXVKVy3BiopMCCU//arcgis//rest//services//Address_Point//FeatureServer//0"
addr = morpcParcels.gdf_from_services(url = addr_url, crs=None).set_crs('NAD83')
if not os.path.exists('./input_data/delaware_data/addr/'):
    os.makedirs('./input_data/delaware_data/addr/')
addr.to_file("./input_data/delaware_data/addr/delaware_addr.shp")

# %%
parcels_url = "https://services2.arcgis.com//ziXVKVy3BiopMCCU//arcgis//rest//services//Parcel//FeatureServer//0"
parcels = morpcParcels.gdf_from_services(url = parcels_url, crs=None).set_crs('NAD83')
if not os.path.exists('./input_data/delaware_data/parcels/'):
    os.makedirs('./input_data/delaware_data/parcels/')
parcels.to_file("./input_data/delaware_data/parcels/delaware_parcels.shp")

# %% [markdown]
# # Read Data

# %%
parcels = pyogrio.read_dataframe("./input_data/delaware_data/parcels/delaware_parcels.shp")

# %%
addr = pyogrio.read_dataframe("./input_data/delaware_data/addr/delaware_addr.shp")

# %%
units = parcels[['OBJECTID', 'geometry']].sjoin(addr[['LSN', 'geometry']]).drop(columns='index_right').groupby('OBJECTID').agg({'LSN':'count'}).rename(columns={'LSN':'UNITS'})

# %%
parcels['CLASS'] = [str(x) for x in parcels['CLASS']]
parcels['YRBUILT'] = [int(x) for x in parcels['YRBUILT']]
parcels['ACRES'] = [float(x) for x in parcels['ACRES']]

# %%
parcels = parcels.groupby('OBJECTID').agg({
    'CLASS':'first',
    'YRBUILT':'max',
    'ACRES':'max',
    'geometry':'first'
})

# %%
parcels = parcels.join(units).reset_index()

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry', crs='NAD83')

# %%
parcels = parcels.to_crs('epsg:3735')

# %%
parcels.to_file('./output_data/delaware_parcels.gpkg')

# %%
