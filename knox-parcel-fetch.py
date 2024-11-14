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

# %%
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
temp_dir = "./knox_data/parcels/"
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

archive_path = os.path.join(temp_dir, os.path.basename(parcel_path))
with open(archive_path, "wb") as fd:
    fd.write(r.content)

with zipfile.ZipFile(archive_path) as zip:
    for zip_info in zip.infolist():
        zip.extract(zip_info, temp_dir)

# %%
morpcParcels.download_and_unzip_archive(url=cama_path, temp_dir='./knox_data/cama', keep_zip=True)

# %%
cama_url = "https://www.knoxcountyauditor.org/site-links/weights-measures/"
print(f"Retrieving cama file name from {cama_url}"),
driver = webdriver.Edge()
driver.get(cama_url)
driver.find_element(By.LINK_TEXT, "REAL ESTATE").click()
cama_link = driver.find_element(By.XPATH, "//html[1]/body[1]/header[1]/div[1]/div[3]/nav[1]/div[1]/ul[1]/li[2]/div[1]/a[4]")
cama_path = cama_link.get_attribute('href')
driver.close()

# %% jupyter={"source_hidden": true}
all_columns = []
for file in os.listdir('./knox_data/cama/'):
    if not file.endswith('.zip'):
        table = pd.read_csv(os.path.join('./knox_data/cama/', file), sep="|", dtype='str')
        table.columns = [f"{x}_{file}" for x in table.columns.values]
        all_columns.append(morpcParcels.sample_columns_from_df(table))
all_columns = pd.concat(all_columns)

# %% jupyter={"source_hidden": true}
all_columns.to_csv('./knox_data/all_cama_columns.csv')

# %%
r = requests.get(addr_path)
temp_dir = "./knox_data/address/"
if not os.path.exists(temp_dir):
    os.makedirs(temp_dir)

archive_path = os.path.join(temp_dir, os.path.basename(parcel_path))
with open(archive_path, "wb") as fd:
    fd.write(r.content)

with zipfile.ZipFile(archive_path) as zip:
    for zip_info in zip.infolist():
        zip.extract(zip_info, temp_dir)

# %% [markdown]
# # Load Data

# %%
knox_parcels_raw = pyogrio.read_dataframe('./knox_data/parcels/parcels.shp')

# %%
extract = morpcParcels.extract_fields_from_cama('./knox_data/cama/no_filename.zip', filename='MVP1_OH_Extract.txt')
build = morpcParcels.extract_fields_from_cama('./knox_data/cama/no_filename.zip', filename='MVP1_OH_Building.txt')
dwell = morpcParcels.extract_fields_from_cama('./knox_data/cama/no_filename.zip', filename='MVP1_OH_Dwelling.txt')

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
addr_path = "https://co.knox.oh.us/wp-content/uploads/2024/09/AddressPts-9-13-24.zip"

# %%
knox_addr = pyogrio.read_dataframe('./knox_data/address/AddressPts.shp')

# %%
knox_parcels = knox_parcels_raw[['PIN', 'Acres', 'geometry']].set_index('PIN').join(cama.set_index('mpropertyNumber'))

# %%
knox_parcels = knox_parcels.loc[~knox_parcels['mClassificationId'].isna()]

# %%
units = knox_parcels_raw.reset_index().sjoin(knox_addr[['GlobalID', 'geometry']]).groupby('PIN').agg({'GlobalID':'count'}).rename(columns = {'GlobalID':'units'}).sort_values('units', ascending=False)

# %%
knox_parcels = gpd.GeoDataFrame(knox_parcels, geometry='geometry')

# %%
knox_parcels = knox_parcels.join(units)[['geometry', 'mClassificationId', 'macres', 'YearBuilt', 'units']]

# %%
knox_parcels['YearBuilt'] = [pd.to_numeric(x) for x in knox_parcels['YearBuilt']]
knox_parcels['macres'] = [pd.to_numeric(x) for x in knox_parcels['macres']]
knox_parcels['units'] = [pd.to_numeric(x) for x in knox_parcels['units']]

# %%
knox_parcels = knox_parcels.drop_duplicates()


# %%
def get_housing_units_field(table, acres_name, luc_name):
    table[acres_name] = [pd.to_numeric(x) for x in table[acres_name]]
    table[luc_name] = [str(x) for x in table[luc_name]]

    table.loc[(table[acres_name] > .75 ) & (table[luc_name].str.startswith('51')), 'housing_unit_type'] = "SF-LL"
    table.loc[(table[acres_name] <= .75) & (table[luc_name].str.startswith('51')), 'housing_unit_type'] = "SF-SL"
    table.loc[table[luc_name].str.startswith(('52', '53', '54', '55')), 'housing_unit_type'] = "SF-A"
    table.loc[table[luc_name].str.startswith('4'), 'housing_unit_type'] = "MF"
    return(table)


# %%
knox_parcels = get_housing_units_field(knox_parcels, 'macres', 'mClassificationId')

# %%
knox_parcels = knox_parcels.loc[knox_parcels['housing_unit_type']!='nan']

# %%
knox_parcels['geometry'] = knox_parcels['geometry'].centroid
knox_parcels = knox_parcels.loc[~knox_parcels['geometry'].isna()]

# %%
knox_parcels['x'] = [point.x for point in knox_parcels['geometry']]
knox_parcels['y'] = [point.y for point in knox_parcels['geometry']]

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Knox'].to_crs('epsg:3734'), fill="None", color='black')
    + plotnine.geom_jitter(knox_parcels, plotnine.aes(x='x', y='y', size = 'units', fill = 'housing_unit_type'), color="None")
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
