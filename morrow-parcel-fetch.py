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

# %%
INPUT_DIR = "./input_data"

inputDir = os.path.normpath(INPUT_DIR)
if not os.path.exists(inputDir):
    os.makedirs(inputDir)

jurisdictionsPartsRaw = morpc.load_spatial_data(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, layerName=JURISDICTIONS_PARTS_FEATURECLASS_LAYER, archiveDir=inputDir)
jurisdictionsPartsRaw = jurisdictionsPartsRaw.to_crs('epsg:3735')

# %%
addr_url = 'https://engineer.co.morrow.oh.us/arcgis/rest/services/Morrow_OtherLayers/MapServer/1'

# %%
r = requests.get('https://engineer.co.morrow.oh.us/arcgis/rest/services/Morrow_OtherLayers/MapServer/1/query?outFields=*&where=1%3D1&f=geojson&resultOffset=0&resultRecordCount=1000')

# %%
r.json()

# %%
morpcParcels.gdf_from_services(addr_url)

# %%
