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
# # Madison Parcel Data

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

# %%
morpcParcels.download_and_unzip_archive(url = "http://ftp1.co.madison.oh.us:81/Auditor/Data/GIS/", filename='parcels.zip', temp_dir='./input_data/madison_data/parcels/')

# %%
morpcParcels.download_and_unzip_archive(url ='https://gis1.oit.ohio.gov/LBRS/_downloads/', filename='MAD_ADDS.zip', temp_dir='./input_data/delaware_data/addr', keep_zip=True)

# %%
morpcParcels.download_and_unzip_archive(url='http://madison-public.issg.io/api/Document/', filename='PublicRecordsExtract.zip', temp_dir='./input_data/madison_data/cama/', keep_zip=True)

# %%
parcels_raw = gpd.read_file('./input_data/madison_data/parcels/parcels.shp')

# %%
addr_raw = gpd.read_file('./input_data/delaware_data/addr/MAD_ADDS.shp')

# %%

# %%
parcels = parcels_raw[['TAXPIN', 'geometry']].to_crs('epsg:3735')

# %%
parcels = parcels.dissolve(by='TAXPIN')

# %%
addr = addr_raw[['LSN', 'geometry']].to_crs('epsg:3735')

# %%
units = parcels.sjoin(addr[['LSN', 'geometry']]).groupby('TAXPIN').agg({'LSN':'count'}).rename(columns={'LSN':'UNITS'})

# %%
build = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Building.xml', columns=['Parcel_Number', 'Card', 'Year_Built', 'Year_Effective']).set_index('Parcel_Number')
dwell = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Dwelling.xml', columns=['Parcel_Number', 'Card', 'Year_Built', 'Year_Effective']).set_index('Parcel_Number')
yrbuilt = pd.concat([build, dwell]).dropna()
yrbuilt['Year_Effective'] = [x if y==None else y for x, y in zip(yrbuilt['Year_Built'], yrbuilt['Year_Effective'])]
yrbuilt = yrbuilt.groupby('Parcel_Number').agg({'Year_Effective':'max'})

# %%
land_use = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Appraisal.xml', columns=['Parcel_Number', 'Land_Use_Code']).set_index('Parcel_Number')

# %%
appr_tot = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Appraisal.xml', columns=['Parcel_Number', 'Total_Appraised_Improvement', 'Total_Appraised_Land']).set_index('Parcel_Number')
appr_tot['APPRTOT'] = [int(x) + int(y) for x, y in zip(appr_tot['Total_Appraised_Improvement'], appr_tot['Total_Appraised_Land'])]
appr_tot = appr_tot[['APPRTOT']]

# %%
acres = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel.xml', columns=['Parcel_Number', 'Acres']).set_index('Parcel_Number')

# %%
parcels = parcels.join([land_use, yrbuilt, acres, units, appr_tot]).drop_duplicates().reset_index()

# %%
parcels = parcels.rename(columns = {
    'TAXPIN':'OBJECTID',
    'Acres':'ACRES',
    'Land_Use_Code':'CLASS',
    'Year_Effective':'YRBUILT', 
    'Units':'UNITS'})

# %%
parcels

# %%
parcels.to_file('./output_data/madison_parcels.gpkg')

# %%
