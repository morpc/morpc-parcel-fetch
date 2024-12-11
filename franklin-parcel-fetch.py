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

# %% [markdown]
# # Download Data

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

# %%
init_url = "https://apps.franklincountyauditor.com/Outside_User_Files/"
r = requests.get(os.path.dirname(init_url))
pattern = r'<A HREF="/Outside_User_Files/([^/]+/)">'
years = re.findall(pattern, r.text)
temp_url = os.path.join(init_url, years[-1])
r = requests.get(temp_url)
pattern = f'<A HREF="/Outside_User_Files/{years[-1]}([^/]+/)">'
dirs = re.findall(pattern, r.text)
appraisal_dirs = []
for dir in dirs:
    if "Appraisal" in dir:
        appraisal_dirs.append(dir)
appraisal_url = os.path.join(temp_url, appraisal_dirs[-1])
accounting_dirs = []
for dir in dirs:
    if "Accounting" in dir:
        accounting_dirs.append(dir)
accounting_url = os.path.join(temp_url, accounting_dirs[-1])

# %%
if not os.path.exists('./input_data/franklin_data/cama/'):
    os.makedirs('./input_data/franklin_data/cama/')

# %%
morpcParcels.download_and_unzip_archive(url=appraisal_url, filename='Excel.zip', temp_dir='./input_data/franklin_data/cama/appraisal/', keep_zip=True)

# %%
morpcParcels.download_and_unzip_archive(url=accounting_url, filename='Excel.zip', temp_dir='./input_data/franklin_data/cama/accounting/', keep_zip=True)

# %% [markdown]
# ## Parcel Geometry

# %%
parcels_raw = pyogrio.read_dataframe('./input_data/franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='TaxParcel_CondoUnitStack_LGIM')

# %%
parcels = parcels_raw[['PARCELID', 'CLASSCD', 'ACRES', 'geometry']]

# %%
parcels['PARCELID'] = [x + '-00' for x in parcels['PARCELID']]

# %%
parcels = parcels.loc[~parcels['PARCELID'].str.startswith(('VNP', 'OUT', 'W', 'w', 'R', 'r'))]

# %%
parcels = parcels.dissolve(by='PARCELID')

# %% [markdown]
# ## Unit counts from address

# %%
addr_raw = pyogrio.read_dataframe('./input_data/franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='LBRS_AddressPoints')

# %%
units = parcels.sjoin(addr_raw[['LSN', 'geometry']]).groupby('PARCELID').agg({'LSN':'count'}).rename(columns={'LSN':'units'})

# %% [markdown]
# ## Year Built from CAMA

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

# %%
parcels = parcels.join(units)

# %%
parcels = parcels.join(yrbuilt.set_index('PARCEL ID')).reset_index()

# %%
parcels = parcels.rename(columns={
    'PARCELID':'OBJECTID',
    'YRBLT':'YRBUILT',
    'CLASSCD':'CLASS',
    'units':'UNITS'
})

# %%
parcels = parcels.to_crs('epsg:3735')

# %%
parcels.to_file('./output_data/franklin_parcels.gpkg')

# %%
