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

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='parcels.zip', temp_dir='./input_data/fairfield_data/parcels/')

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='addresses.zip', temp_dir='./input_data/fairfield_data/addr/')

# %% [markdown]
# ## Read Data

# %%
parcels_raw = pyogrio.read_dataframe('./input_data/fairfield_data/parcels/parcels.shp')

# %%
addr_raw = pyogrio.read_dataframe('./input_data/fairfield_data/addr/addresses.shp')

# %%
parcels = parcels_raw[['PARID', 'ACRES', 'LUC', 'YRBLT', 'geometry']]
parcels = parcels.rename(columns = {'YRBLT':'YRBUILT', 'PARID':'OBJECTID', 'LUC':'CLASS'})
parcels = parcels.loc[~parcels['OBJECTID'].isna()]

# %%
units = (addr_raw[['LSN', 'geometry']].sjoin(parcels[['OBJECTID', 'geometry']])
         .groupby('OBJECTID').agg({
             'LSN':'count'
         }).rename(columns = {'LSN':'UNITS'})
        )

# %%
parcels = parcels.set_index('OBJECTID').join(units).reset_index()

# %%
parcels = parcels.to_crs('epsg:3735')

# %%
parcels.to_file('./output_data/fairfield_parcels.gpkg')

# %%
