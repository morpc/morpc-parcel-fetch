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
# ## SETUP

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
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By


sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %% [markdown]
# ## Franklin

# %%
url = 'https://apps.franklincountyauditor.com/GIS_Shapefiles/CurrentExtracts/'

# %%
r = requests.get(os.path.dirname(url))
files = re.findall(r'.zip">(.*?.zip)<', r.text)
for file in files:
    if "GeoDataBase" in file:
        filename = file

# %%
morpcParcels.download_and_unzip_archive(url=url, filename=filename, temp_dir='./franklin_data')

# %%
franklin_parcels = pyogrio.read_dataframe("./franklin_data/Output/FCA_SDE_Web_Prod.gdb", layer='TaxParcel_CondoUnitStack_LGIM', columns=['PARCELID','ACRES','CLASSCD','RESYRBLT'])

# %%
franklin_parcels

# %%
franklin_parcels.rename(columns = {})

# %% [markdown]
# ## Delaware

# %%
url = "https://services2.arcgis.com/ziXVKVy3BiopMCCU/arcgis/rest/services/Parcel/FeatureServer/0"

# %%
delaware_parcels = morpcParcels.gdf_from_services(url = url, fieldIds=["CLASS","YRBUILT","ACRES"])
if not os.path.exists('./delaware_data/'):
    os.makedirs('./delaware_data/')
delaware_parcels.to_file("./delaware_data/delaware_parcels.gpkg", driver='GPKG')

# %% [markdown]
# ## Union

# %%
morpcParcels.download_and_unzip_archive(url='https://www2.co.union.oh.us/EngineerGIS/', filename='Property_Parcels.zip', temp_dir='./union_data')

# %%
union_parcels = pyogrio.read_dataframe('union_data/temp/Property_Parcels.shp', columns=['GISNO', 'CAMANO', 'ClassNumbe', 'Total_Acre'])

# %% [markdown]
# ## Licking

# %%
morpcParcels.download_and_unzip_archive(url='https://lickingcounty.gov/civicax/filebank/blobdload.aspx?BlobID=106060', temp_dir='./licking_data/')

# %%
licking_parcels = pyogrio.read_dataframe('./licking_data/PARCELS.shp', columns=['AUD_PIN', 'TAX_ACRE'])

# %% [markdown]
# ## Madison

# %%
morpcParcels.download_and_unzip_archive(url = "http://ftp1.co.madison.oh.us:81/Auditor/Data/GIS/", filename='parcels.zip', temp_dir='./madison_data/parcels/')

# %%

# %%
morpcParcels.download_and_unzip_archive(url='http://madison-public.issg.io/api/Document/', filename='PublicRecordsExtract.zip', temp_dir='./madison_data/cama/', keep_zip=True)

# %%
madison_cama = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Building.xml', columns=['Parcel_Number_U', 'Year_Built'])

# %% [markdown]
# ## Pickaway

# %%
url = 'https://services6.arcgis.com/FhJ42byMw3LmPYCN/arcgis/rest/services/Parcels_Search/FeatureServer/1/'

# %%
pickaway_parcels = morpcParcels.gdf_from_services(url=url, fieldIds=['PARCELCAMA', 'PPAcres'])

# %% [markdown]
# ## Fairfield

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='parcels.zip', temp_dir='./fairfield_data/parcels/')

# %%
fairfield_parcels = pyogrio.read_dataframe('./fairfield_data/parcels/parcels.shp', columns=['ACRES', 'LUC', 'YRBLT'])

# %% [markdown]
# ## Morrow

# %%
morpcParcels.download_and_unzip_archive(url='https://engineer.co.morrow.oh.us/DataDownload/ParcelExport/',filename='Morrow_Parcels.zip', temp_dir='./morrow_data/parcels/')

# %%
morrow_parcels = pyogrio.read_dataframe('./morrow_data/parcels/Morrow_Parcels.shp', columns=['Name', 'A_LandUse', 'A_Acreage'])

# %%
morpcParcels.download_and_unzip_archive(url='https://share.pivotpoint.us/oh/morrow/cama/', filename='morrowoh.zip', temp_dir='./morrow_data/cama', keep_zip=True)

# %%
morpcParcels.sample_fields_from_zipped_cama(zip_path='./morrow_data/cama/morrowoh.zip', filename='GovernmaxBuildingExtract.txt')

# %%
morrow_cama = morpcParcels.extract_fields_from_cama(zip_path='./morrow_data/cama/morrowoh.zip', filename='GovernmaxBuildingExtract.txt', columns=['PropertyNumber', 'UseCode', 'YearBuilt'])

# %% [markdown]
# ## Knox

# %%
parcel_url = "https://co.knox.oh.us/tax-map-and-gis/archive-and-downloads/"
print(f"Retrieving parcel file name from {parcel_url}")
driver = webdriver.Edge()
driver.get(parcel_url)
parcel_link = driver.find_element(By.XPATH, "//html[1]/body[1]/div[2]/section[2]/div[1]/div[1]/div[1]/section[2]/div[1]/div[2]/div[1]/div[2]/div[1]/ul[1]/li[5]/a[1]")
parcel_path = parcel_link.get_attribute('href')
driver.close()

# %%
morpcParcels.download_and_unzip_archive(url='https://co.knox.oh.us/wp-content/uploads/2024/09/', filename='parcels-9-13-24.zip', temp_dir='./knox_data/parcels')

# %%
knox_parcels = pyogrio.read_dataframe('./knox_data/parcels/parcels.shp', columns=['PIN', 'Acres'])

# %%
knox_parcels

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
morpcParcels.download_and_unzip_archive(url=cama_path, temp_dir='./knox_data/cama', keep_zip=True)

# %%
knox_cama = morpcParcels.extract_fields_from_cama('./knox_data/cama/no_filename.zip', filename='MVP1_OH_TransferHistory.txt', columns=['SourceParcel','YearBuilt', 'UseCode'])

# %%
knox_cama

# %% [markdown]
# ## Logan

# %%
logan_parcels = morpcParcels.gdf_from_services('https://services9.arcgis.com/mFxO7gBbusFBQ5o9/ArcGIS/rest/services/Logan_County_Parcels/FeatureServer/12', fieldIds=['Parcel_Num', 'Land_Use_C', 'Acres'])

# %%
if not os.path.exists('./logan_data/'):
    os.makedirs('./logan_data/')
logan_parcels.to_file("./logan_data/logan_parcels.gpkg", driver='GPKG')

# %%
logan_parcels

# %%
morpcParcels.download_and_unzip_archive('https://realestate.co.logan.oh.us/api/Document/', filename='PublicRecordsExcel.zip', temp_dir='./logan_data/cama/', keep_zip=True)

# %%
year_built = morpcParcels.extract_fields_from_cama(zip_path='./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Dwelling.xlsx',columns=['Parcel Number', 'Year Built']).set_index('Parcel Number')

# %%
land_use = morpcParcels.extract_fields_from_cama(zip_path='./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Appraisal.xlsx',columns=['Parcel Number', 'Land Use Code']).set_index('Parcel Number')

# %%
acres = morpcParcels.extract_fields_from_cama(zip_path='./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel.xlsx', columns=['Parcel Number', 'Acres']).set_index('Parcel Number')

# %%
logan_parcels.set_index('Parcel_Num')[['geometry']].join([year_built, land_use, acres])

# %% [markdown]
# ## Ross

# %%
ross_parcels = morpcParcels.gdf_from_services('https://services7.arcgis.com/IQSUQhVBDHAkRlWe/ArcGIS/rest/services/parcel_joined/FeatureServer/0')

# %%
ross_parcels[['PARCEL_NO', 'PPYearBuilt', 'PPClassNumber', 'PPAcres', 'geometry']]
