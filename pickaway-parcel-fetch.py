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

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %%
parcels_raw = morpcParcels.gdf_from_services('https://services6.arcgis.com/FhJ42byMw3LmPYCN/arcgis/rest/services/parcel_joined/FeatureServer/0', crs=None)

# %%
parcels_raw = parcels_raw.set_crs('NAD83')

# %%
addr_raw = morpcParcels.gdf_from_services('https://services6.arcgis.com/FhJ42byMw3LmPYCN/ArcGIS/rest/services/Addresses/FeatureServer/0', crs=None)

# %%
addr_raw = addr_raw.set_crs('NAD83')

# %%
parcels = parcels_raw[['Parcel', 'PPYearBuilt', 'PPClassNumber', 'PPAcres', 'geometry']]

# %%
parcels = parcels.reset_index().drop(columns='index')

# %%
parcels['PPYearBuilt'] = [x.split('|')[-1] if x!=None else None for x in parcels['PPYearBuilt']]

# %%
parcels = parcels.rename(columns={
    'Parcel':'OBJECTID',
    'PPAcres':'ACRES',
    'PPYearBuilt':'YRBUILT',
    'PPClassNumber':'CLASS',
})

# %%
units = parcels.sjoin(addr_raw[['LSN', 'geometry']].to_crs(parcels.crs)).groupby('OBJECTID').agg({'LSN':'count'}).rename(columns={'LSN':'UNITS'})

# %%
parcels = parcels.set_index('OBJECTID').join(units).reset_index()

# %%
parcels = parcels.to_crs('epsg:3735')

# %%
parcels.to_file('./output_data/pickaway_parcels.gpkg')

# %%
