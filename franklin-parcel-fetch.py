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
franklin_units_filt.crs

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
franklin_geo = pyogrio.read_dataframe('./franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='TaxParcel_CondoUnitStack_LGIM')

# %%
franklin_geo = franklin_geo[['PARCELID', 'CLASSCD', 'geometry']]

# %%
franklin_geo['PARCELID'] = [x + '-00' for x in franklin_geo['PARCELID']]

# %%
franklin_geo['geometry'] = franklin_geo['geometry'].copy().centroid

# %%
franklin_geo = franklin_geo.rename(columns = {'PARCELID':'PARCEL ID', 'CLASSCD':'LUC'})

# %% [markdown]
# ## Commercial Unit counts

# %%
unit_raw = pl.read_excel("C:\\Users\\jinskeep\\OneDrive - Mid-Ohio Regional Planning Commission\\Local Repo\\morpc-parcel-fetch\\franklin_data\\com_units.xlsx").to_pandas()

# %% [markdown]
# ## Building card number for commercial parcels

# %%
build_raw = pl.read_excel(os.path.join('./franklin_data/appraisal/Build.xlsx')).to_pandas()

# %% [markdown]
# ## Dwelling card number and living units for dwellings. 

# %%
dwelling_raw = pl.read_excel(os.path.join('./franklin_data/appraisal/Dwelling.xlsx')).to_pandas()

# %% [markdown]
# ## Parcel land use codes and acerage

# %%
acc_parcel_raw = pl.read_excel(os.path.join('./franklin_data/accounting/Parcel.xlsx')).to_pandas()

# %% [markdown]
# ## Rentals for back up unit counts if not included in commerical units

# %%
rental_raw = pl.read_excel(os.path.join('./franklin_data/accounting/RentalContact.xlsx')).to_pandas()

# %% [markdown]
# # Apartments and other commercial housing

# %%
luc_filter = [401, 402, 403, 404, 414, 419, 431, 475, 510, 511, 512, 513, 514, 515, 520, 521, 522, 523, 524, 525, 530, 531, 532, 534, 535, 550, 551, 552, 553, 560, 570, 571, 572, 585, 586, 587, 588, 589, 591, 592, 593]

# %%
unit = unit_raw[['Parcel Number', 'Building Card Number', 'Rental Units', 'Year Built', 'Year Effective', 'Units']].copy()
unit['Units'] =[pd.to_numeric(x) if pd.isna(y) else pd.to_numeric(y) for x, y in zip(unit['Rental Units'], unit['Units'])]
unit['Year Effective'] =[pd.to_numeric(x) if pd.isna(y) else pd.to_numeric(y) for x, y in zip(unit['Year Built'], unit['Year Effective'])]
unit = unit.rename(columns = {'Parcel Number':'PARCEL ID', 'Building Card Number':'CARD', 'Year Effective':'EFFYR'})
unit = unit.loc[unit['Units']>0].set_index(['PARCEL ID', 'CARD'])

# %%
unit = unit.groupby('PARCEL ID').agg({
    "Units":'sum',
    'EFFYR':'max'
})

# %%
unit = unit.join(acc_parcel_raw[['PARCEL ID', 'LUC', 'GisAcres']].set_index('PARCEL ID').copy())
unit['LUC'] = [pd.to_numeric(x) for x in unit['LUC']]

# %%
unit = unit.join(franklin_geo.set_index('PARCEL ID'), rsuffix='_geo')

# %%
unit = unit.reset_index()[['PARCEL ID', 'EFFYR', 'Units', 'LUC', 'GisAcres', 'geometry']].rename(columns = {'Units':'UNITS', 'GisAcres':'ACRES'}).drop_duplicates()
unit['source'] = 'unit'

# %% [markdown]
# ## Dwellings and residential units

# %%
dwelling = dwelling_raw[['PARCEL ID', 'CARD', 'YRBLT', 'EFFYR', 'LIVUNITS']].copy()

# %%
dwelling['EFFYR'] = [x if pd.isna(y) else y for x, y in zip(dwelling['YRBLT'], dwelling['EFFYR'])]
dwelling = dwelling.rename(columns={'LIVUNITS':'UNITS'})
dwelling['UNITS'] = [pd.to_numeric(x) for x in dwelling['UNITS']]

# %%
dwelling = (dwelling[['PARCEL ID', 'CARD', 'UNITS', 'EFFYR']]
 .drop_duplicates()
 .groupby(['PARCEL ID']).agg({
     'UNITS':'sum',
     'EFFYR':'max'
 }).reset_index())

# %%
dwelling = dwelling.set_index('PARCEL ID').join(acc_parcel[['PARCEL ID', 'LUC', 'GisAcres']].set_index('PARCEL ID'))

# %%
dwelling = dwelling.join(franklin_geo.set_index('PARCEL ID'), rsuffix='_geo')

# %%
dwelling = dwelling.reset_index()[['PARCEL ID', 'EFFYR', 'UNITS', 'LUC', 'GisAcres', 'geometry']].rename(columns = {'GisAcres':'ACRES'})
dwelling['source'] = 'dwelling'

# %%
franklin_units = pd.concat([unit, dwelling])

# %% [markdown]
# ## Filter for Land use code and years

# %%
franklin_units = franklin_units.loc[franklin_units['LUC'].isin(luc_filter)]

# %%
franklin_units_filt = franklin_units.loc[franklin_units['EFFYR']>=2023]
franklin_units_filt = franklin_units_filt.loc[franklin_units_filt['UNITS']>0]
franklin_units_filt = franklin_units_filt.loc[~franklin_units_filt['geometry'].isna()]

# %%
franklin_units_filt['LUC'] = [str(x) for x in franklin_units_filt['LUC']]

# %%
franklin_units_filt.loc[(franklin_units_filt['ACRES'] > .75 )& (franklin_units_filt['LUC'].str.startswith('51')), 'housing_unit_type'] = "SF-LL"
franklin_units_filt.loc[(franklin_units_filt['ACRES'] <= .75) & (franklin_units_filt['LUC'].str.startswith('51')), 'housing_unit_type'] = "SF-SL"
franklin_units_filt.loc[franklin_units_filt['LUC'].str.startswith(('52', '53', '54', '55')), 'housing_unit_type'] = "SF-A"
franklin_units_filt.loc[franklin_units_filt['LUC'].str.startswith('4'), 'housing_unit_type'] = "MF"
franklin_units_filt = gpd.GeoDataFrame(franklin_units_filt, geometry='geometry').to_crs('epsg:3735')

# %%
franklin_units_filt.groupby(['housing_unit_type', 'EFFYR']).agg({'UNITS':'sum'})

# %%
franklin_units_filt.sort_values('UNITS', ascending=False)

# %%
franklin_units_filt.sjoin(jurisdictionsPartsRaw[['PLACECOMBO', 'geometry']]).groupby(['PLACECOMBO']).agg({'UNITS':'sum'})

# %%
franklin_units_plot = franklin_units_filt.set_index('PARCEL ID').copy()
franklin_units_plot['x'] = [point.x for point in franklin_units_plot['geometry']]
franklin_units_plot['y'] = [point.y for point in franklin_units_plot['geometry']]

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Franklin'], fill="None", color='black')
    + plotnine.geom_jitter(franklin_units_plot, plotnine.aes(x='x', y='y', size = 'UNITS', fill = 'housing_unit_type'), color="None")
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
   + plotnine.scale_size_radius(range=(.2,5), breaks = (1,50, 100, 250, 400))
)

# %%
