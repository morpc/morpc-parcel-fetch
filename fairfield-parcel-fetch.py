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
# # Fairfield Parcel Data

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
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='parcels.zip', temp_dir='./fairfield_data/parcels/')

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='addresses.zip', temp_dir='./fairfield_data/parcels/')

# %%
itables.show(morpcParcels.sample_columns_from_df(fairfield_parcels_raw))

# %%
fairfield_parcels_raw = pyogrio.read_dataframe('./fairfield_data/parcels/parcels.shp')
fairfield_parcels = fairfield_parcels_raw[['PARID', 'ACRES', 'LUC', 'YRBLT', 'geometry']]
fairfield_parcels = fairfield_parcels.rename(columns = {'ACRES':'acres','LUC':'land_use','YRBLT':'year_built'})
fairfield_parcels = fairfield_parcels.to_crs('3735')
fairfield_parcels['county'] = 'Fairfield'

# %%
fairfield_addr_raw = pyogrio.read_dataframe('./fairfield_data/parcels/addresses.shp')

# %%
units = (fairfield_addr_raw[['LSN', 'geometry']].sjoin(fairfield_parcels[['PARID', 'geometry']])
 .groupby('PARID').agg({
     'LSN':'count'
 })
 .sort_values('LSN', ascending=False)
 .rename(columns = {'LSN':'units'}))

# %%
fairfield_parcels = fairfield_parcels.set_index('PARID').join(units)

# %%
fairfield_parcels['geometry'] = fairfield_parcels['geometry'].centroid
fairfield_parcels['x'] = [point.x for point in fairfield_parcels['geometry']]
fairfield_parcels['y'] = [point.y for point in fairfield_parcels['geometry']]

# %%
fairfield_parcels['year_built'] = [pd.to_numeric(x) for x in fairfield_parcels['year_built']]

# %%
fairfield_parcels = fairfield_parcels.sjoin(jurisdictionsPartsRaw[['PLACECOMBO', 'geometry']])

# %%
pd.DataFrame(fairfield_parcels.loc[fairfield_parcels['year_built']>2020].groupby(['PLACECOMBO', 'year_built']).agg({'units':'sum'})).rename(columns={0:'units'}).reset_index().pivot(index='PLACECOMBO', columns='year_built',values= 'units')


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
fairfield_parcels = get_housing_units_field(fairfield_parcels, "acres", 'land_use')

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Fairfield'], fill="None", color='black')
    + plotnine.geom_jitter(fairfield_parcels, plotnine.aes(x='x', y='y', size = 'units', fill = 'housing_unit_type'), color="None")
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
   + plotnine.scale_size_radius(range=(.2,5), breaks = (1,50, 100, 250, 400))
 + plotnine.guides(size=plotnine.guide_legend(override_aes={'color':'black'}))
)

# %%
