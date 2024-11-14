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
# # Franklin Sub-county Units from Parcels

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
geo_url = 'https://apps.franklincountyauditor.com/GIS_Shapefiles/CurrentExtracts/'

# %%
r = requests.get(os.path.dirname(geo_url))
files = re.findall(r'.zip">(.*?.zip)<', r.text)
for file in files:
    if "GeoDataBase" in file:
        filename = file

# %%
morpcParcels.download_and_unzip_archive(url=geo_url, filename=filename, temp_dir='./franklin_data')

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
morpcParcels.download_and_unzip_archive(url=appraisal_url, filename='Excel.zip', temp_dir='./franklin_data/appraisal/', keep_zip=True)

# %%
morpcParcels.download_and_unzip_archive(url=accounting_url, filename='Excel.zip', temp_dir='./franklin_data/accounting/', keep_zip=True)

# %% [markdown]
# Commercial units are only available online through the web reporter download (https://audr-apps.franklincountyohio.gov/Reporter). Select Parcel Number, Primary Land Use Code, Building Card Number, and Units.

# %% [markdown]
# # Read Data

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

# %% [markdown]
# ## Parcel Geometry

# %%
geo = pyogrio.read_dataframe('./franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='TaxParcel_CondoUnitStack_LGIM')

# %%
geo = geo[['PARCELID', 'CLASSCD', 'geometry']]

# %%
geo['PARCELID'] = [x + '-00' for x in geo['PARCELID']]

# %% [markdown]
# ## Unit counts from address

# %%
addr = pyogrio.read_dataframe('./franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='LBRS_AddressPoints')

# %%
units = geo.sjoin(addr[['LSN', 'geometry']]).groupby('PARCELID').agg({'LSN':'count'}).rename(columns={'LSN':'units'})

# %% [markdown]
# ## Building card number for commercial parcels

# %%
build_raw = pl.read_excel(os.path.join('./franklin_data/appraisal/Build.xlsx')).to_pandas()
build = build_raw[['PARCEL ID', 'CARD', 'YRBLT']].copy()
build = (build[['PARCEL ID', 'CARD', 'YRBLT']]
 .drop_duplicates()
 .groupby(['PARCEL ID']).agg({
     'YRBLT':'max'
 }).reset_index())

# %% [markdown]
# ## Dwelling card number and living units for dwellings. 

# %%
dwelling_raw = pl.read_excel(os.path.join('./franklin_data/appraisal/Dwelling.xlsx')).to_pandas()
dwelling = dwelling_raw[['PARCEL ID', 'CARD', 'YRBLT']].copy()
dwelling = (dwelling[['PARCEL ID', 'CARD', 'YRBLT']]
 .drop_duplicates()
 .groupby(['PARCEL ID']).agg({
     'YRBLT':'max'
 }).reset_index())

# %% [markdown]
# ## Parcel land use codes and acerage

# %%
parcel_raw = pl.read_excel(os.path.join('./franklin_data/accounting/Parcel.xlsx')).to_pandas()
parcel = parcel_raw[['PARCEL ID', 'LUC', 'GisAcres']]

# %%
parcel = parcel.set_index('PARCEL ID').join(pd.concat([dwelling, build]).set_index('PARCEL ID'))

# %%
parcel = parcel.join(geo[['PARCELID', 'geometry']].set_index('PARCELID'))

# %%
parcel = parcel.join(units)


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
parcels = get_housing_units_field(parcel, 'GisAcres', 'LUC')

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry')
parcels['geometry'] = parcels['geometry'].centroid
parcels = parcels.loc[~parcels['geometry'].isna()]
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels = parcels.loc[parcels['housing_unit_type']!='nan']

# %%
parcels = parcels.reset_index()

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Franklin'], fill="None", color='black')
    + plotnine.geom_jitter(parcels.loc[parcels['YRBLT']>=2019], plotnine.aes(x='x', y='y', size = 'units', fill = 'housing_unit_type'), color="None")
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
   + plotnine.scale_size_radius(range=(.2,5), breaks = (1,50, 100, 250, 500))
   + plotnine.guides(size=plotnine.guide_legend(override_aes={'color':'black'}))
)

# %%
