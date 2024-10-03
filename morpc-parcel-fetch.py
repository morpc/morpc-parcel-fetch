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
import plotnine
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %% [markdown]
# ## Download Inputs

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

# %% [markdown]
# ## Licking

# %%
morpcParcels.download_and_unzip_archive(url='https://lickingcounty.gov/civicax/filebank/blobdload.aspx?BlobID=106060', temp_dir='./licking_data/')

# %% [markdown]
# ## Madison

# %%
morpcParcels.download_and_unzip_archive(url = "http://ftp1.co.madison.oh.us:81/Auditor/Data/GIS/", filename='parcels.zip', temp_dir='./madison_data/parcels/')

# %%
morpcParcels.download_and_unzip_archive(url='http://madison-public.issg.io/api/Document/', filename='PublicRecordsExtract.zip', temp_dir='./madison_data/cama/', keep_zip=True)

# %%
itables.show(morpcParcels.sample_fields_from_zipped_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Building.xml'))

# %% [markdown]
# ## Pickaway

# %%
url = 'https://services6.arcgis.com/FhJ42byMw3LmPYCN/arcgis/rest/services/Parcels_Search/FeatureServer/1/'

# %%
pickaway_parcels = morpcParcels.gdf_from_services(url=url, fieldIds=['PARCELCAMA', 'PPAcres'])

# %%
if not os.path.exists('./pickaway_data/'):
    os.makedirs('./pickaway_data/')
pickaway_parcels.to_file("./pickaway_data/pickaway_parcels.gpkg", driver='GPKG')

# %% [markdown]
# ## Fairfield

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='parcels.zip', temp_dir='./fairfield_data/parcels/')

# %% [markdown]
# ## Morrow

# %%
morpcParcels.download_and_unzip_archive(url='https://engineer.co.morrow.oh.us/DataDownload/ParcelExport/',filename='Morrow_Parcels.zip', temp_dir='./morrow_data/parcels/')

# %%
morpcParcels.download_and_unzip_archive(url='https://share.pivotpoint.us/oh/morrow/cama/', filename='morrowoh.zip', temp_dir='./morrow_data/cama', keep_zip=True)

# %%
morpcParcels.sample_fields_from_zipped_cama(zip_path='./morrow_data/cama/morrowoh.zip', filename='GovernmaxBuildingExtract.txt')

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

# %% [markdown]
# ## Logan

# %%
logan_parcels = morpcParcels.gdf_from_services('https://services9.arcgis.com/mFxO7gBbusFBQ5o9/ArcGIS/rest/services/Logan_County_Parcels/FeatureServer/12', fieldIds=['Parcel_Num', 'Land_Use_C', 'Acres'])

# %%
if not os.path.exists('./logan_data/'):
    os.makedirs('./logan_data/')
logan_parcels.to_file("./logan_data/logan_parcels.gpkg", driver='GPKG')

# %%
morpcParcels.download_and_unzip_archive('https://realestate.co.logan.oh.us/api/Document/', filename='PublicRecordsExcel.zip', temp_dir='./logan_data/cama/', keep_zip=True)

# %% [markdown]
# ## Ross

# %%
ross_parcels = morpcParcels.gdf_from_services('https://services7.arcgis.com/IQSUQhVBDHAkRlWe/ArcGIS/rest/services/parcel_joined/FeatureServer/0')

# %%
if not os.path.exists('./ross_data/'):
    os.makedirs('./ross_data/')
ross_parcels.to_file("./ross_data/ross_parcels.gpkg", driver='GPKG')

# %% [markdown]
# ## Read and shape inputs

# %%
franklin_parcels = pyogrio.read_dataframe('./franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='TaxParcel_CondoUnitStack_LGIM')
franklin_parcels = franklin_parcels.rename(columns = {'PARCELID':'parcel_id','ACRES':'acres','CLASSCD':'land_use','RESYRBLT':'year_built'}).set_index('parcel_id')
franklin_parcels = franklin_parcels[['acres', 'land_use', 'year_built', 'geometry']]
franklin_parcels = franklin_parcels.to_crs('3735')
franklin_parcels['county'] = 'Franklin'

# %%
delaware_parcels = pyogrio.read_dataframe('./delaware_data/delaware_parcels.gpkg')
delaware_parcels = delaware_parcels.rename(columns = {'ACRES':'acres','CLASS':'land_use','YRBUILT':'year_built'})
delaware_parcels = delaware_parcels[['acres', 'land_use', 'year_built', 'geometry']]
delaware_parcels = delaware_parcels.to_crs('3735')
delaware_parcels['county'] = 'Delaware'

# %%
union_parcels = pyogrio.read_dataframe('union_data/temp/Property_Parcels.shp', columns=['GISNO', 'CAMANO', 'ClassNumbe', 'Total_Acre'])
union_parcels = union_parcels.rename(columns = {'GISNO':'parcel_id','Total_Acre':'acres','ClassNumbe':'land_use'}).set_index('parcel_id')
union_parcels = union_parcels[['acres', 'land_use', 'geometry']]
union_parcels = union_parcels.to_crs('3735')
union_parcels['county'] = 'Union'

# %%
licking_parcels = pyogrio.read_dataframe('./licking_data/PARCELS.shp', columns=['AUD_PIN', 'TAX_ACRE'])
licking_parcels = licking_parcels.rename(columns = {'AUD_PIN':'parcel_id','TAX_ACRE':'acres'}).set_index('parcel_id')
licking_parcels = licking_parcels[['acres', 'geometry']]
licking_parcels = licking_parcels.to_crs('3735')
licking_parcels['county'] = 'Licking'

# %%
madison_parcels = gpd.read_file('./madison_data/parcels/parcels.shp')
madison_parcels = madison_parcels[['TAXPIN', 'geometry']].set_index('TAXPIN')
land_use = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Appraisal.xml', columns=['Parcel_Number', 'Land_Use_Code']).set_index('Parcel_Number')
year_built = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Dwelling.xml', columns=['Parcel_Number', 'Year_Built']).set_index('Parcel_Number')
acres = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel.xml', columns=['Parcel_Number', 'Acres']).set_index('Parcel_Number')
madison_parcels = madison_parcels.join([land_use, year_built, acres])
madison_parcels = madison_parcels.rename(columns = {'Acres':'acres','Land_Use_Code':'land_use','Year_Built':'year_built'})
madison_parcels = madison_parcels[['acres', 'land_use', 'year_built', 'geometry']]
madison_parcels = madison_parcels.to_crs('3735')
madison_parcels['county'] = 'Madison'

# %%
pickaway_parcels = gpd.read_file("./pickaway_data/pickaway_parcels.gpkg")
pickaway_parcels = pickaway_parcels.rename(columns = {'PARCELCAMA':'parcel_id', 'PPAcres':'acres'}).set_index('parcel_id')
pickaway_parcels = pickaway_parcels[['acres', 'geometry']]
pickaway_parcels = pickaway_parcels.to_crs('3735')
pickaway_parcels['county'] = 'Pickaway'

# %%
fairfield_parcels = pyogrio.read_dataframe('./fairfield_data/parcels/parcels.shp', columns=['ACRES', 'LUC', 'YRBLT'])
fairfield_parcels = fairfield_parcels.rename(columns = {'ACRES':'acres','LUC':'land_use','YRBLT':'year_built'})
fairfield_parcels = fairfield_parcels[['acres', 'land_use', 'year_built', 'geometry']]
fairfield_parcels = fairfield_parcels.to_crs('3735')
fairfield_parcels['county'] = 'Fairfield'

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
knox_cama = morpcParcels.extract_fields_from_cama('./knox_data/cama/no_filename.zip', filename='MVP1_OH_TransferHistory.txt', columns=['SourceParcel','YearBuilt', 'UseCode'])
knox_cama['SourceParcel'] = [re.sub('[^0-9]', '', x) for x in knox_cama['SourceParcel']]
knox_cama['UseCode'] = [re.split(' ', str(x))[0] for x in knox_cama['UseCode']]
knox_cama = knox_cama.set_index('SourceParcel')
knox_parcels = pyogrio.read_dataframe('./knox_data/parcels/parcels.shp').set_index('PIN')
knox_parcels = knox_cama.join(knox_parcels[['geometry', 'Acres']])
knox_parcels = knox_parcels.rename(columns = {'Acres':'acres','UseCode':'land_use','YearBuilt':'year_built'})
knox_parcels = knox_parcels[['acres', 'land_use', 'year_built', 'geometry']]
knox_parcels = gpd.GeoDataFrame(knox_parcels, geometry='geometry').to_crs('3735')
knox_parcels['county'] = 'Knox'

# %%
logan_parcels = gpd.read_file("./logan_data/logan_parcels.gpkg")
year_dwell = morpcParcels.extract_fields_from_cama(zip_path='./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Dwelling.xlsx',columns=['Parcel Number', 'Year Built']).set_index('Parcel Number')
year_build = morpcParcels.extract_fields_from_cama(zip_path='./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Building.xlsx',columns=['Parcel Number', 'Year Built']).set_index('Parcel Number')
year_built = pd.concat([year_build, year_dwell]).dropna()
land_use = morpcParcels.extract_fields_from_cama(zip_path='./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel Appraisal.xlsx',columns=['Parcel Number', 'Land Use Code']).set_index('Parcel Number')
logan_cama = land_use.join(year_built, how='outer').dropna()
acres = morpcParcels.extract_fields_from_cama(zip_path='./logan_data/cama/PublicRecordsExcel.zip', filename='Parcel.xlsx', columns=['Parcel Number', 'Acres']).set_index('Parcel Number')
logan_cama = logan_cama.join(acres)
logan_parcels = logan_cama.join(logan_parcels.set_index('Parcel_Num')[['geometry']])
logan_parcels = logan_parcels.rename(columns = {'Acres':'acres','Land Use Code':'land_use','Year Built':'year_built'})
logan_parcels = logan_parcels[['acres', 'land_use', 'year_built', 'geometry']]
logan_parcels['land_use'] = [str(x) for x in logan_parcels['land_use']]
logan_parcels = gpd.GeoDataFrame(logan_parcels, geometry='geometry').to_crs('3735')
logan_parcels['county'] = 'Logan'

# %%
ross_parcels = gpd.read_file('./ross_data/ross_parcels.gpkg')
ross_parcels = ross_parcels[['PARCEL_NO', 'PPYearBuilt', 'PPClassNumber', 'PPAcres', 'geometry']]
ross_parcels = ross_parcels.rename(columns = {'PARCEL_NO':'parcel_id','PPYearBuilt':'year_built','PPClassNumber':'land_use','PPAcres':'acres'}).set_index('parcel_id')
ross_parcels = ross_parcels[['acres', 'land_use', 'year_built', 'geometry']]
ross_parcels['land_use'] = [str(x) for x in ross_parcels['land_use']]
ross_parcels['year_built'] = [re.split('\|', str(x))[0] for x in ross_parcels['year_built']]
ross_parcels = ross_parcels.to_crs('3735')
ross_parcels['county'] = 'Ross'

# %%
all_parcels = pd.concat([franklin_parcels, delaware_parcels, madison_parcels, fairfield_parcels, morrow_parcels, knox_parcels, logan_parcels, ross_parcels])

# %%
landuse_filter = ['401', '402', '403', '511', '512', '513', '514', '515', '520', '521', '522', '523', '524', '525', '530', '531', '532', '533', '534', '534', '535', '540', '550']

# %%
all_parcels['acres'] = pd.to_numeric(all_parcels['acres'])

# %%
all_parcels['year_built'] = [pd.to_numeric(x, errors='coerce') for x in all_parcels['year_built']]
all_parcels['year_built'] = all_parcels['year_built'].replace(0, None)

# %%
all_parcels = all_parcels.loc[all_parcels['land_use'].isin(landuse_filter)]

# %%
all_parcels.loc[all_parcels['acres'] > 1 & all_parcels['land_use'].str.startswith('51'), 'housing_unit_type'] = "SF-LL"
all_parcels.loc[all_parcels['acres'] <= 1 & all_parcels['land_use'].str.startswith('51'), 'housing_unit_type'] = "SF-SL"
all_parcels.loc[all_parcels['land_use'].str.startswith(('52', '53', '54')), 'housing_unit_type'] = "SF-A"
all_parcels.loc[all_parcels['land_use'].str.startswith('4'), 'housing_unit_type'] = "MF"


# %%
all_parcels.plot()

# %%
all_parcels['geometry'] = all_parcels['geometry'].centroid

# %%
COUNTIES_FEATURECLASS_SOURCE_URL = "https://www2.census.gov/geo/tiger/GENZ2020/shp/cb_2020_us_county_500k.zip"

# %%
morpcCounties = morpc.countyLookup()

# %%
countiesRaw = gpd.read_file(COUNTIES_FEATURECLASS_SOURCE_URL)

# %%
counties = countiesRaw.copy()
counties = counties \
    .filter(items=["GEOID", "NAME", "geometry"], axis="columns") \
    .astype({
        "GEOID":"string",
        "NAME":"string"
    }) \
    .set_index("GEOID") \
    .loc[[morpcCounties.get_id(x) for x in morpc.CONST_REGIONS["15-County Region"]]]
counties.head()

# %%
if(counties.crs.to_epsg() != 3735):
    print("Reprojecting from EPSG:{} to EPSG:3735".format(counties.crs.to_epsg()))
    counties = counties.to_crs(epsg=3735)

# %%
(plotnine.ggplot()
    + plotnine.geom_map(counties, color='white', fill='lightgrey')
    + plotnine.geom_map(all_parcels, plotnine.aes(fill='housing_unit_type'), color = None)
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank()
    )
    + plotnine.scale_fill_brewer(type='qual', palette=2)
)

# %%
pd.DataFrame(all_parcels.groupby('county').count()['housing_unit_type'])

# %%
x = all_parcels.loc[all_parcels['housing_unit_type']!='nan']
pd.DataFrame(x.groupby(['county', 'housing_unit_type']).count()['geometry'])

# %%
all_parcels.loc[all_parcels['acres']>0, 'acres'].describe()

# %%
all_parcels.loc[all_parcels['acres']>0][['county', 'acres']].groupby('county').agg('describe')

# %%
all_parcels[['county', 'year_built']].groupby('county').agg('describe')

# %%
