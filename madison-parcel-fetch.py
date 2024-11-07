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
# # Madison Parcel Data

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
madison_addr = pyogrio.read_dataframe("C:\\Users\\jinskeep\\OneDrive - Mid-Ohio Regional Planning Commission\\Local Repo\\morpc-parcel-fetch\\madison_data\\ogrip_addr\\MAD_ADDS\\MAD_ADDS.shp")

# %%
madison_parcels_raw = gpd.read_file('./madison_data/parcels/parcels.shp')


# %%
def get_land_use_codes(filename):
    d = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename=filename)
    d = d[['Property_ID', 'Parcel_Number', 'Land_Use_Code']].drop_duplicates()
    return(d)


# %%
all_d = []
for file in ['Parcel Appraisal.xml']:
    d = get_land_use_codes(file)
    d['source'] = file
    all_d.append(d)
land_use = pd.concat(all_d).drop_duplicates()
land_use = land_use[['Parcel_Number', 'Land_Use_Code']].set_index('Parcel_Number')

# %%
year_build = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Building.xml', columns=['Parcel_Number', 'Card', 'Year_Built', 'Year_Effective']).set_index('Parcel_Number')
year_dwell = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Dwelling.xml', columns=['Parcel_Number', 'Card', 'Year_Built', 'Year_Effective']).set_index('Parcel_Number')
year_built = pd.concat([year_build, year_dwell]).dropna()
year_built['Year_Effective'] = [x if y==None else y for x, y in zip(year_built['Year_Built'], year_built['Year_Effective'])]

# %%
units_build = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Building Composite.xml', columns=['Parcel_Number', 'Units']).set_index('Parcel_Number')
units_build['Units'] = [int(x) for x in units_build['Units']]
units_dwell = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Dwelling.xml', columns=['Parcel_Number', 'Card', 'Units_Designed', 'Units_Converted'])
units_dwell['Units'] = [int(x) if y=='0' else int(y) for x, y in zip(units_dwell['Units_Designed'], units_dwell['Units_Converted'])]

# %%
units_dwell = units_dwell.groupby('Parcel_Number').agg({'Units':'sum'})
units = pd.concat([units_build, units_dwell]).dropna()

# %%
acres = morpcParcels.extract_fields_from_cama(zip_path='./madison_data/cama/PublicRecordsExtract.zip', filename='Parcel.xml', columns=['Parcel_Number', 'Acres']).set_index('Parcel_Number')

# %%
madison_parcels = madison_parcels_raw[['TAXPIN', 'geometry']].set_index('TAXPIN').join([land_use, year_built, acres, units])

# %%
madison_parcels = madison_parcels.rename(columns = {'Acres':'acres','Land_Use_Code':'land_use','Year_Built':'year_built', 'Units':'units'})
madison_parcels = madison_parcels[['acres', 'land_use', 'year_built', 'units', 'geometry']]
madison_parcels = madison_parcels.to_crs('3735')
madison_parcels['county'] = 'Madison'

# %%
madison_parcels['geometry'] = madison_parcels['geometry'].centroid
madison_parcels = madison_parcels.loc[~madison_parcels['geometry'].isna()]

# %%
madison_parcels['x'] = [point.x for point in madison_parcels['geometry']]
madison_parcels['y'] = [point.y for point in madison_parcels['geometry']]

# %%
madison_parcels = madison_parcels.loc[~madison_parcels['land_use'].isna()]
madison_parcels = madison_parcels.loc[~madison_parcels['year_built'].isna()]
madison_parcels = madison_parcels.loc[~madison_parcels['units'].isna()]


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
madison_parcels = get_housing_units_field(madison_parcels, 'acres', 'land_use')

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Madison'], fill="None", color='black')
    + plotnine.geom_jitter(madison_parcels, plotnine.aes(x='x', y='y', size = 'units', fill = 'housing_unit_type'), color="None")
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
   + plotnine.scale_size_radius(range=(.2,10), breaks = (1,10, 25, 50))
     + plotnine.guides(size=plotnine.guide_legend(override_aes={'color':'black'}))
)

# %%
