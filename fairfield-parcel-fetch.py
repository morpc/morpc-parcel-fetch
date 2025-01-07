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
# # Fairfield Parcel Data

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

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %% [markdown]
# ## Archive Data

# %% [markdown]
# Download and archive parcels and addresses

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='parcels.zip', temp_dir='./input_data/fairfield_data/parcels/')

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='addresses.zip', temp_dir='./input_data/fairfield_data/addr/')

# %% [markdown]
# ## Read Data

# %% [markdown]
# import data from archived copies.

# %%
parcels_raw = pyogrio.read_dataframe('./input_data/fairfield_data/parcels/parcels.shp')

# %%
addr_raw = pyogrio.read_dataframe('./input_data/fairfield_data/addr/addresses.shp')

# %% [markdown]
# Drop unneeded columns, rename columns, and drop rows without IDs.

# %%
parcels = parcels_raw[['PARID', 'ACRES', 'LUC', 'YRBLT', 'APRLAND', 'APRBLDG', 'geometry']]
parcels['APPRTOT'] = parcels['APRLAND'] + parcels['APRBLDG']
parcels = parcels.rename(columns = {'YRBLT':'YRBUILT', 'PARID':'OBJECTID', 'LUC':'CLASS'})
parcels = parcels.loc[~parcels['OBJECTID'].isna()]

# %%
parcels['YRBUILT'] = [pd.to_numeric(x, errors='coerce') for x in parcels['YRBUILT']]
parcels['APPRTOT'] = [pd.to_numeric(x, errors='coerce') for x in parcels['APPRTOT']]

# %% [markdown]
# Group identical geometries and remove duplicates. 

# %%
parcels = parcels.groupby('geometry').agg({'OBJECTID':'first', 'YRBUILT':'max', 'CLASS':'first', 'APPRTOT':'max'}).reset_index()
parcels = gpd.GeoDataFrame(parcels, geometry='geometry')

# %% [markdown]
# Get units from spatial join of addresses to parcels.

# %%
units = (addr_raw[['LSN', 'geometry']].sjoin(parcels[['OBJECTID', 'geometry']])
         .groupby('OBJECTID').agg({
             'LSN':'count'
         }).rename(columns = {'LSN':'UNITS'})
        )

# %% [markdown]
# Join units

# %%
parcels = parcels.set_index('OBJECTID').join(units).reset_index()

# %% [markdown]
# Apply CRS

# %%
parcels = parcels.to_crs('epsg:3735')

# %% [markdown]
# Output data

# %%
parcels.to_file('./output_data/fairfield_parcels.gpkg')
