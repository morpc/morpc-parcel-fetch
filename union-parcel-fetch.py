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
# # Union County Parcels

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

# %%
parcels_raw = morpcParcels.gdf_from_services('https://www7.co.union.oh.us/unioncountyohio/rest/services/ParcelAllSaleinfoBuildingInfo/MapServer/1', crs=None)

# %%
addr_raw = morpcParcels.gdf_from_services('https://www7.co.union.oh.us/unioncountyohio/rest/services/Address/FeatureServer/0', crs=None)

# %%
parcels_raw = parcels_raw.set_crs('NAD83')

# %%
addr_raw = addr_raw.set_crs('NAD83')

# %%
parcels = parcels_raw[['ParcelNo', 'PARCELCLASS', 'yearBuilt', 'Acreage', 'appraisedTotalValue', 'geometry']].drop_duplicates()

# %%
parcels = parcels.rename(columns={
    'ParcelNo':'OBJECTID',
    'Acreage':'ACRES',
    'yearBuilt':'YRBUILT',
    'PARCELCLASS':'CLASS',
    'appraisedTotalValue':'APPRTOT'
})

# %%
parcels['ACRES'] = [pd.to_numeric(x, errors='coerce') for x in parcels['ACRES']]
parcels['YRBUILT'] = [pd.to_numeric(x, errors='coerce') for x in parcels['YRBUILT']]
parcels['OBJECTID'] = [str(pd.to_numeric(x, errors='coerce')) for x in parcels['OBJECTID']]
parcels['APPRTOT'] = [pd.to_numeric(x, errors='coerce') for x in parcels['APPRTOT']]

# %%
parcels = parcels.groupby('geometry').agg({'OBJECTID':'first','CLASS':'first','YRBUILT':'max', 'ACRES':'max', 'APPRTOT':'max'}).reset_index()

# %%
parcels = parcels.loc[~parcels['OBJECTID'].isna()]

# %%
parcels = parcels.groupby(['OBJECTID', 'geometry']).agg({'CLASS':'first', 'ACRES':'max','YRBUILT':'max', 'APPRTOT':'max'}).drop('nan').reset_index()

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry')

# %%
units = parcels.sjoin(addr_raw[['AddressID', 'geometry']].to_crs(parcels.crs)).groupby('OBJECTID').agg({'AddressID':'count'}).rename(columns={'AddressID':'UNITS'})

# %%
parcels = parcels.set_index('OBJECTID').join(units).reset_index()

# %%
parcels = parcels.to_crs('epsg:3735')

# %%
parcels.to_file('./output_data/union_parcels.gpkg')
