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
# # Read Data

# %%
franklin_geo = pyogrio.read_dataframe('./franklin_data/Output/FCA_SDE_Web_Prod.gdb/', layer='TaxParcel_CondoUnitStack_LGIM')

# %%
franklin_geo = franklin_geo[['PARCELID', 'CLASSCD', 'geometry']]

# %%
franklin_geo['PARCELID'] = [x + '-00' for x in franklin_geo['PARCELID']]

# %%
franklin_geo['geometry'] = franklin_geo['geometry'].copy().centroid

# %%
unit = pl.read_excel("C:\\Users\\jinskeep\\Downloads\\WebReporter-2024-10-30.xlsx").to_pandas()

# %%
build = pl.read_excel(os.path.join('./franklin_data/appraisal/Build.xlsx')).to_pandas()

# %%
dwelling = pl.read_excel(os.path.join('./franklin_data/appraisal/Dwelling.xlsx')).to_pandas()

# %%
land = pl.read_excel(os.path.join('./franklin_data/appraisal/Land.xlsx')).to_pandas()

# %%
app_parcel = pl.read_excel(os.path.join('./franklin_data/appraisal/Parcel.xlsx')).to_pandas()

# %%
acc_parcel = pl.read_excel(os.path.join('./franklin_data/accounting/Parcel.xlsx')).to_pandas()

# %%
rental = pl.read_excel(os.path.join('./franklin_data/accounting/RentalContact.xlsx')).to_pandas()

# %% [markdown]
# ## Filter for apartments and other commercial housing

# %%
luc_filter = [401, 402, 403, 404, 414, 419, 431, 475]

# %%
apt_luc_acres = acc_parcel['LUC', ]

# %%
build['EFFYR'] = [x if pd.isna(y) else y for x, y in zip(build['YRBLT'], build['EFFYR'])]

# %%
build = build[['PARCEL ID', 'EFFYR', 'USETYPE']]
build = build.loc[build['USETYPE']=='011 - APARTMENT'].drop(columns = 'USETYPE')
build = build.drop_duplicates()

# %%
unit = unit.loc[~unit['Units'].isna()].set_index('Parcel Number')

# %%
build = build.set_index('PARCEL ID').join(unit).rename(columns = ({'Units':'UNITS'})).reset_index()

# %%
build

# %%
dwelling['EFFYR'] = [x if pd.isna(y) else y for x, y in zip(dwelling['YRBLT'], dwelling['EFFYR'])]

# %%
dwelling = (dwelling[['PARCEL ID', 'CARD', 'LIVUNITS', 'EFFYR']]
 .drop_duplicates()
 .groupby(['PARCEL ID', 'CARD']).agg({
     'LIVUNITS':'sum',
     'EFFYR':'max'
 }).reset_index()
        .rename(columns={'LIVUNITS':'UNITS'})
        .drop(columns = 'CARD'))

# %%
dwelling['UNITS'] = [int(x) for x in dwelling['UNITS']]

# %%
franklin_units = pd.concat([build, dwelling]).groupby(['PARCEL ID']).agg({'UNITS':'sum','EFFYR':'max'})

# %%
acc_parcel = acc_parcel[['PARCEL ID', 'LUC', 'GisAcres']].rename(columns={'GisAcres':'ACRES'}).set_index('PARCEL ID')
acc_parcel['LUC'] = [pd.to_numeric(x, errors='coerce') for x in acc_parcel['LUC']]

# %%
franklin_units = franklin_units.join(acc_parcel)

# %%
franklin_units = gpd.GeoDataFrame(franklin_units.join(franklin_geo.set_index('PARCELID'), how='left'), geometry='geometry')

# %%
luc_filter_res = [510, 511, 512, 513, 514, 515, 520, 521, 522, 523, 524, 525, 530, 531, 532, 534, 535, 550, 551, 552, 553, 560, 570, 571, 572, 585, 586, 587, 588, 589, 591, 592, 593]

# %%
franklin_units_filt = franklin_units.loc[franklin_units['CLASSCD'].isin([str(x) for x in luc_filter])]
franklin_units_filt = franklin_units_filt.loc[franklin_units_filt['EFFYR']>=2022]
franklin_units_filt = franklin_units_filt.loc[franklin_units_filt['UNITS']>0]

# %%
franklin_units_filt['LUC'] = [str(x) for x in franklin_units_filt['LUC']]

# %%
franklin_units_filt.loc[(franklin_units_filt['ACRES'] > .75 )& (franklin_units_filt['LUC'].str.startswith('51')), 'housing_unit_type'] = "SF-LL"
franklin_units_filt.loc[(franklin_units_filt['ACRES'] <= .75) & (franklin_units_filt['LUC'].str.startswith('51')), 'housing_unit_type'] = "SF-SL"
franklin_units_filt.loc[franklin_units_filt['LUC'].str.startswith(('52', '53', '54', '55')), 'housing_unit_type'] = "SF-A"
franklin_units_filt.loc[franklin_units_filt['LUC'].str.startswith('4'), 'housing_unit_type'] = "MF"

# %%
franklin_units_filt.groupby(['housing_unit_type', 'EFFYR']).agg({'UNITS':'sum'})

# %%
build.loc[build['PARCEL ID'] == '010-001082-00']

# %%
dwelling.loc[dwelling['PARCEL ID'] == '010-001082-00']

# %%
franklin_units_filt.sort_values('UNITS', ascending=False)

# %%
(plotnine.ggplot()
    + plotnine.geom_map(franklin_units_filt, plotnine.aes(size = 'UNITS', fill = 'housing_unit_type'))
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
)

# %%
