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
STANDARD_GEO_VINTAGE = 2023
JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH = "../morpc-censustiger-standardize/output_data/morpc-standardgeos-census-{}.gpkg".format(STANDARD_GEO_VINTAGE)
JURISDICTIONS_PARTS_FEATURECLASS_LAYER = "JURIS-COUNTY"
print("Data: {0}, layer={1}".format(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, JURISDICTIONS_PARTS_FEATURECLASS_LAYER))

# %%
INPUT_DIR = "./input_data"

inputDir = os.path.normpath(INPUT_DIR)
if not os.path.exists(inputDir):
    os.makedirs(inputDir)

jurisdictionsPartsRaw = morpc.load_spatial_data(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, layerName=JURISDICTIONS_PARTS_FEATURECLASS_LAYER, archiveDir=inputDir)
jurisdictionsPartsRaw = jurisdictionsPartsRaw.to_crs('epsg:3735')

# %%
morpcParcels.download_and_unzip_archive(url='https://www2.co.union.oh.us/EngineerGIS/', filename='Property_Parcels.zip', temp_dir='./union_data')
union_parcels = pyogrio.read_dataframe('union_data/temp/Property_Parcels.shp')
union_parcels = union_parcels.to_crs('epsg:3735')

# %%
morpcParcels.sample_columns_from_df(union_parcels.drop(columns='geometry'))

# %%
pd.DataFrame(union_parcels.loc[union_parcels['ClassNumbe']!=0].groupby(['Parcel']).size()).rename(columns = {0:'size'}).sort_values('size', ascending=False)

# %%
itables.show(morpcParcels.sample_columns_from_df(union_parcels.loc[union_parcels['Parcel']=='3900240530040'].drop(columns='geometry')))

# %%
union_parcels[['Parcel', 'Total_Acre', 'ClassNumbe', ]]

# %%
