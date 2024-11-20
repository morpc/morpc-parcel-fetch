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
# # Knox County Parcel Data

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
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

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
jurisdictionsPartsRaw = jurisdictionsPartsRaw.to_crs('epsg:3735')

# %%
parcel_url = "https://co.knox.oh.us/tax-map-and-gis/archive-and-downloads/"
print(f"Retrieving parcel file name from {parcel_url}")
driver = webdriver.Edge()
driver.get(parcel_url)
parcel_link = driver.find_element(By.XPATH, "//html[1]/body[1]/div[2]/section[2]/div[1]/div[1]/div[1]/section[2]/div[1]/div[2]/div[1]/div[2]/div[1]/ul[1]/li[5]/a[1]")
parcel_path = parcel_link.get_attribute('href')
driver.close()

# %%
r = requests.get(parcel_path)
temp_dir = "./input_data/knox_data/parcels/"
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

archive_path = os.path.join(temp_dir, os.path.basename(parcel_path))
with open(archive_path, "wb") as fd:
    fd.write(r.content)

with zipfile.ZipFile(archive_path) as zip:
    for zip_info in zip.infolist():
        if zip_info.filename.startswith('_'):
            continue
        if zip_info.filename.endswith('/'):
            continue
        zip_info.filename = zip_info.filename.split('/')[-1]
        zip.extract(zip_info, temp_dir)

# %%
cama_url = "https://www.knoxcountyauditor.org/site-links/weights-measures/"
print(f"Retrieving cama file name from {cama_url}"),
driver = webdriver.Edge()
driver.get(cama_url)
driver.find_element(By.LINK_TEXT, "REAL ESTATE").click()
cama_link = driver.find_element(By.XPATH, "//html[1]/body[1]/header[1]/div[1]/div[3]/nav[1]/div[1]/ul[1]/li[2]/div[1]/a[4]")
cama_path = cama_link.get_attribute('href')
driver.close()

# %%
r = requests.get(cama_path)
temp_dir = "./input_data/knox_data/cama/"
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

cama_filename = "CAMADatabase.zip"

archive_path = os.path.join(temp_dir, cama_filename)
with open(archive_path, "wb") as fd:
    fd.write(r.content)

with zipfile.ZipFile(archive_path) as zip:
    for zip_info in zip.infolist():
        zip.extract(zip_info, temp_dir)

# %%
addr_path = "https://co.knox.oh.us/wp-content/uploads/2024/09/AddressPts-9-13-24.zip"

# %%
r = requests.get(addr_path)
temp_dir = "./input_data/knox_data/address/"
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

archive_path = os.path.join(temp_dir, os.path.basename(addr_path))
with open(archive_path, "wb") as fd:
    fd.write(r.content)

with zipfile.ZipFile(archive_path) as zip:
    for zip_info in zip.infolist():
        zip.extract(zip_info, temp_dir)

# %% [markdown]
# # Load Data

# %%
parcels_raw = pyogrio.read_dataframe('./input_data/knox_data/parcels/parcels.shp')

# %%
parcels_raw.crs

# %%
addr = pyogrio.read_dataframe('./input_data/knox_data/address/AddressPts.shp')

# %%
extract = pd.read_csv('./input_data/knox_data/cama/MVP1_OH_Extract.txt', sep="|", dtype='str')
build = pd.read_csv('./input_data/knox_data/cama/MVP1_OH_Building.txt', sep="|", dtype='str')
dwell = pd.read_csv('./input_data/knox_data/cama/MVP1_OH_Dwelling.txt', sep="|", dtype='str')

# %%
extract = extract[['mpropertyNumber', 'CardCount', 'mClassificationId', 'macres']].set_index('mpropertyNumber')

# %%
build  = build[['PropertyNumber', 'CardNumber', 'YearBuilt']].set_index('PropertyNumber')

# %%
dwell = dwell[['PropertyNumber', 'CardNumber', 'YearBuilt']].set_index('PropertyNumber')

# %%
cama = extract.join(pd.concat([dwell, build])).reset_index()

# %%
cama['mpropertyNumber'] = [re.sub('[^0-9]', '', x) for x in cama['mpropertyNumber']]

# %%
parcels = parcels_raw[['PIN', 'Acres', 'geometry']].set_index('PIN').join(cama.set_index('mpropertyNumber'))

# %%
parcels = parcels.loc[~parcels['mClassificationId'].isna()]

# %%
units = parcels_raw.reset_index().sjoin(addr[['GlobalID', 'geometry']]).groupby('PIN').agg({'GlobalID':'count'}).rename(columns = {'GlobalID':'units'}).sort_values('units', ascending=False)

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry')

# %%
parcels = parcels.join(units)[['geometry', 'mClassificationId', 'macres', 'YearBuilt', 'units']]

# %%
parcels['YearBuilt'] = [pd.to_numeric(x) for x in parcels['YearBuilt']]
parcels['macres'] = [pd.to_numeric(x) for x in parcels['macres']]
parcels['units'] = [pd.to_numeric(x) for x in parcels['units']]

# %%
parcels = parcels.drop_duplicates()

# %%
parcels = morpcParcels.get_housing_unit_type_field(parcels, 'macres', 'mClassificationId')

# %%
parcels = parcels.loc[parcels['TYPE']!='nan']

# %%
parcels['geometry'] = parcels['geometry'].centroid
parcels = parcels.loc[~parcels['geometry'].isna()]

# %%
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels.columns.values

# %%
parcels = parcels.rename(columns={
    'mClassificationId':'OBJECTID',
    'macres':'ACRES',
    'YearBuilt':'YRBUILT',
    'units':'UNITS',
})

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Knox'].to_crs('epsg:3734'), fill="None", color='black')
    + plotnine.geom_jitter(parcels, plotnine.aes(x='x', y='y', size = 'UNITS', fill = 'TYPE'), color="None")
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
   + plotnine.scale_size_radius(range=(.2,10), breaks = (1, 10, 50, 100))
     + plotnine.guides(size=plotnine.guide_legend(override_aes={'color':'black'}))
)

# %%
if not os.path.exists('./output_data/hu_type_from_parcels.gpkg'):
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg')
else:
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg', mode='a')

# %%
