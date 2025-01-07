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
# # Pickaway County Parcel Data

# %%
import os
import requests
import pyogrio
import re
import zipfile
import io
import geopandas as gpd
import pandas as pd
import polars as pl
from polars import selectors as cs
import xml.etree.ElementTree as ET
import random
import itables
import plotnine
import sys

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %% [markdown]
# Download and archive raw parcel and address file.

# %%
parcels_url = "https://services6.arcgis.com/FhJ42byMw3LmPYCN/arcgis/rest/services/parcel_joined/FeatureServer/0"
parcels_raw = morpcParcels.gdf_from_services(url = parcels_url, crs=None).set_crs('NAD83')
if not os.path.exists('./input_data/pickaway_data/parcels/'):
    os.makedirs('./input_data/pickaway_data/parcels/')
parcels_raw.to_file("./input_data/pickaway_data/parcels/pickaway_parcels.shp")

# %%
addr_url = "https://services6.arcgis.com/FhJ42byMw3LmPYCN/ArcGIS/rest/services/Addresses/FeatureServer/0"
addr_raw = morpcParcels.gdf_from_services(url = addr_url, crs=None).set_crs('NAD83')
if not os.path.exists('./input_data/delaware_data/addr/'):
    os.makedirs('./input_data/delaware_data/addr/')
addr_raw.to_file("./input_data/delaware_data/addr/delaware_addr.shp")

# %% [markdown]
# Dropping unneeded columns

# %%
parcels = parcels_raw[['Parcel', 'PPYearBuilt', 'PPClassNumber', 'PPAcres', 'PPTotalValue', 'geometry']]

# %%
parcels = parcels.reset_index().drop(columns='index')

# %% [markdown]
# Correct year built column

# %%
parcels['PPYearBuilt'] = [x.split('|')[-1] if x!=None else None for x in parcels['PPYearBuilt']]

# %%
parcels = parcels.rename(columns={
    'Parcel':'OBJECTID',
    'PPAcres':'ACRES',
    'PPYearBuilt':'YRBUILT',
    'PPClassNumber':'CLASS',
    'PPTotalValue':'APPRTOT'
})

# %% [markdown]
# Get units from spatial join of addresses to parcels

# %%
units = parcels.sjoin(addr_raw[['LSN', 'geometry']].to_crs(parcels.crs)).groupby('OBJECTID').agg({'LSN':'count'}).rename(columns={'LSN':'UNITS'})

# %% [markdown]
# Join units to parcels

# %%
parcels = parcels.set_index('OBJECTID').join(units).reset_index()

# %% [markdown]
# Change crs and export

# %%
parcels = parcels.to_crs('epsg:3735')

# %%
parcels.to_file('./output_data/pickaway_parcels.gpkg')

# %%
