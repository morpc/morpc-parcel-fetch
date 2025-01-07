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
# # Fetch Franklin County Parcel Data

# %% [markdown]
# ## Introduction

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
# # Download Data

# %% [markdown]
# Download Franklin parcels from ftp site:
#
# - Get the filename for the GeoDataBase with correct date from "https://apps.franklincountyauditor.com/GIS_Shapefiles/CurrentExtracts"
# - Create archive folder if not already made.
# - Download and archive raw copy of file.
#
#

# %%
ftp_url = 'https://apps.franklincountyauditor.com/GIS_Shapefiles/CurrentExtracts/'
r = requests.get(os.path.dirname(ftp_url))
files = re.findall(r'.zip">(.*?.zip)<', r.text)
for file in files:
    if "GeoDataBase" in file:
        filename = file

# %%
if not os.path.exists('./input_data/franklin_data/'):
    os.makedirs('./input_data/franklin_data/')

morpcParcels.download_and_unzip_archive(url=ftp_url, filename=filename, temp_dir='./input_data/franklin_data/')

# %% [markdown]
# Download Franklin cama from ftp site:
#
# - Get correct file names for CAMA files.
# - Create archive folder.
# - Archive appraisal database to get building and dwelling cards, we will extract year built column from these. 

# %%
init_url = "https://apps.franklincountyauditor.com/Outside_User_Files/"
r = requests.get(os.path.dirname(init_url))
pattern = r'<A HREF="/Outside_User_Files/([^/]+/)">'
years = re.findall(pattern, r.text) ## Finding the folder names for years
temp_url = os.path.join(init_url, years[-1]) ## Get URL for the correct year
r = requests.get(temp_url)
pattern = f'<A HREF="/Outside_User_Files/{years[-1]}([^/]+/)">'
dirs = re.findall(pattern, r.text)
appraisal_dirs = []
for dir in dirs:
    if "Appraisal" in dir:
        appraisal_dirs.append(dir)
appraisal_url = os.path.join(temp_url, appraisal_dirs[-1])

# %%
if not os.path.exists('./input_data/franklin_data/cama/'):
    os.makedirs('./input_data/franklin_data/cama/')

# %%
morpcParcels.download_and_unzip_archive(url=appraisal_url, filename='Excel.zip', temp_dir='./input_data/franklin_data/cama/appraisal/', keep_zip=True)

# %% [markdown]
# ## Parcel Geometry

# %% [markdown]
# Import raw parcel files from archived file.

# %%
parcels_raw = pyogrio.read_dataframe('./input_data/franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='TaxParcel_CondoUnitStack_LGIM')

# %% [markdown]
# Drop unneeded columns. 

# %%
parcels = parcels_raw[['PARCELID', 'CLASSCD', 'ACRES', 'TOTVALUEBASE', 'geometry']].copy()

# %% [markdown]
# Add -00 to match CAMA data.

# %%
parcels['PARCELID'] = [x + '-00' for x in parcels['PARCELID']]

# %% [markdown]
# Drop parcels without parcel ids (ROW, Water, etc.)

# %%
parcels = parcels.loc[~parcels['PARCELID'].str.startswith(('VNP', 'OUT', 'W', 'w', 'R', 'r'))]

# %% [markdown]
# Remove parcels with same IDs by dissolving them together. 

# %%
parcels = parcels.dissolve(by='PARCELID')

# %% [markdown]
# Groupby geometry to remove the parcels that have the same geometry, get first parcel ID, LUC class code, and largest acreage. 

# %%
parcels = parcels.reset_index().groupby('geometry').agg({'PARCELID':'first', 'CLASSCD':'first', 'ACRES':'max', 'TOTVALUEBASE':'max'}).reset_index().set_index('PARCELID')

# %% [markdown]
# Convert back to the final parcels geodataframe

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry')

# %% [markdown]
# ## Unit counts from address

# %% [markdown]
# Import address points from archived geodataframe

# %%
addr_raw = pyogrio.read_dataframe('./input_data/franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='LBRS_AddressPoints')

# %% [markdown]
# Get units from spatial join of address points which are inside parcels. 

# %%
units = parcels.sjoin(addr_raw[['LSN', 'geometry']]).groupby('PARCELID').agg({'LSN':'count'}).rename(columns={'LSN':'units'})

# %% [markdown]
# ## Year Built from CAMA

# %% [markdown]
# Import building and dwelling data for year built field. Parcels and Card numbers are combined and joined. Building cards represent commercial building data for the buildings on the parcel. Dwelling cars represent the residential buildings on the parcel. Some parcels may have many of either of both. 
#
# These are combined and the most recent year is assigned to the parcel id. 

# %%
building_raw = pl.read_excel(os.path.join('./input_data/franklin_data/cama/appraisal/Build.xlsx')).to_pandas()
building = building_raw[['PARCEL ID', 'CARD', 'YRBLT']].copy()
building = (building[['PARCEL ID', 'CARD', 'YRBLT']]
 .drop_duplicates()
 .groupby(['PARCEL ID']).agg({
     'YRBLT':'max'
 }).reset_index())

# %%
dwelling_raw = pl.read_excel(os.path.join('./input_data/franklin_data/cama/appraisal/Dwelling.xlsx')).to_pandas()
dwelling = dwelling_raw[['PARCEL ID', 'CARD', 'YRBLT']].copy()
dwelling = (dwelling[['PARCEL ID', 'CARD', 'YRBLT']]
 .drop_duplicates()
 .groupby(['PARCEL ID']).agg({
     'YRBLT':'max'
 }).reset_index())

# %%
yrbuilt = pd.concat([dwelling, building]).groupby('PARCEL ID').agg({'YRBLT':'max'}).reset_index()

# %% [markdown]
# ## Join Units and yrbuilt

# %% [markdown]
# Join units and year built data to the parcels.

# %%
parcels = parcels.join(units)

# %%
parcels = parcels.join(yrbuilt.set_index('PARCEL ID')).reset_index()

# %% [markdown]
# Rename columns to match was schema. 

# %%
parcels = parcels.rename(columns={
    'PARCELID':'OBJECTID',
    'YRBLT':'YRBUILT',
    'CLASSCD':'CLASS',
    'units':'UNITS',
    'TOTVALUEBASE':'APPRTOT'
})

# %% [markdown]
# Apply crs.

# %%
parcels = parcels.to_crs('epsg:3735')

# %% [markdown]
# Output data

# %%
if not os.path.exists('./output_data/'):
    os.makedirs('./output_data/')

# %%
parcels.to_file('./output_data/franklin_parcels.gpkg')
