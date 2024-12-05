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
# # Morrow County Parcel Data

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
STANDARD_GEO_VINTAGE = 2023
JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH = "../morpc-censustiger-standardize/output_data/morpc-standardgeos-census-{}.gpkg".format(STANDARD_GEO_VINTAGE)
JURISDICTIONS_PARTS_FEATURECLASS_LAYER = "JURIS-COUNTY"
print("Data: {0}, layer={1}".format(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, JURISDICTIONS_PARTS_FEATURECLASS_LAYER))
INPUT_DIR = "./input_data"
inputDir = os.path.normpath(INPUT_DIR)
if not os.path.exists(inputDir):
    os.makedirs(inputDir)
jurisdictionsPartsRaw = morpc.load_spatial_data(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, layerName=JURISDICTIONS_PARTS_FEATURECLASS_LAYER, archiveDir=inputDir)

# %%
parcel_url = "https://engineer.co.morrow.oh.us/arcgis/rest/services/Morrow_ParcelRelated_new/MapServer/0"

# %%
parcels = morpcParcels.gdf_from_services(parcel_url)

# %%
morpcParcels.download_and_unzip_archive(url='https://share.pivotpoint.us/oh/morrow/cama/', filename='morrowoh.zip', temp_dir='./morrow_data/cama', keep_zip=True)

# %%
morrow_parcels = pyogrio.read_dataframe('./morrow_data/parcels/Morrow_Parcels.shp').set_index('Name')
morrow_building = morpcParcels.extract_fields_from_cama(zip_path='./morrow_data/cama/morrowoh.zip', filename='GovernmaxBuildingExtract.txt', columns=['PropertyNumber', 'UseCode', 'YearBuilt']).set_index('PropertyNumber')
morrow_dwelling = morpcParcels.extract_fields_from_cama(zip_path='./morrow_data/cama/morrowoh.zip', filename='GovernmaxDwellingExtract.txt', columns=['PropertyNumber', 'UseCode', 'YearBuilt']).set_index('PropertyNumber')
morrow_dwelling = morrow_dwelling.join(morrow_parcels[['geometry', 'A_Acreage']]).dropna()
morrow_building = morrow_building.join(morrow_parcels[['geometry', 'A_Acreage']]).dropna()
morrow_parcels = pd.concat([morrow_dwelling, morrow_building])
morrow_parcels = morrow_parcels.rename(columns = {'A_Acreage':'acres','UseCode':'land_use','YearBuilt':'year_built'})
morrow_parcels = morrow_parcels[['acres', 'land_use', 'year_built', 'geometry']]
morrow_parcels = gpd.GeoDataFrame(morrow_parcels, geometry='geometry').to_crs('3735')
morrow_parcels['county'] = 'Morrow'

# %%
morrow_building = morpcParcels.extract_fields_from_cama(zip_path='./morrow_data/cama/morrowoh.zip', filename='GovernmaxBuildingExtract.txt')
morrow_dwelling = morpcParcels.extract_fields_from_cama(zip_path='./morrow_data/cama/morrowoh.zip', filename='GovernmaxDwellingExtract.txt')

# %%
morrow_building.loc[morrow_building['UseCode'].str.startswith('40')==True][['PropertyNumber', 'UseCode', 'UnitCount']].drop_duplicates()

# %%
itables.show(morrow_building.loc[morrow_building['PropertyNumber']=='A01-001-00-235-07'])

# %%
morrow_building['UnitCount'] = [pd.to_numeric(x) for x in morrow_building['UnitCount']]

# %%
morrow_building

# %%
morrow_dwelling.groupby('PropertyNumber').agg({'CardNumber':'count'}).sort_values('CardNumber', ascending=False)

# %%
