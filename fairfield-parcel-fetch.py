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
INPUT_DIR = "./input_data"
inputDir = os.path.normpath(INPUT_DIR)
if not os.path.exists(inputDir):
    os.makedirs(inputDir)
jurisdictionsPartsRaw = morpc.load_spatial_data(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, layerName=JURISDICTIONS_PARTS_FEATURECLASS_LAYER, archiveDir=inputDir)

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='parcels.zip', temp_dir='./input_data/fairfield_data/parcels/')

# %%
morpcParcels.download_and_unzip_archive(url='https://www.co.fairfield.oh.us/gis/Fairfield_Data/', filename='addresses.zip', temp_dir='./input_data/fairfield_data/addr/')

# %%
parcels_raw = pyogrio.read_dataframe('./input_data/fairfield_data/parcels/parcels.shp')

# %%
addr_raw = pyogrio.read_dataframe('./input_data/fairfield_data/addr/addresses.shp')

# %%
parcels = parcels_raw[['PARID', 'ACRES', 'LUC', 'YRBLT', 'geometry']]
parcels = parcels.rename(columns = {'YRBLT':'YRBUILT'})

# %%
units = (addr_raw[['LSN', 'geometry']].sjoin(parcels[['PARID', 'geometry']])
 .groupby('PARID').agg({
     'LSN':'count'
 })
 .sort_values('LSN', ascending=False)
 .rename(columns = {'LSN':'UNITS'}))

# %%
parcels = parcels.set_index('PARID').join(units)

# %%
parcels = parcels.to_crs('3735')
parcels['geometry'] = parcels['geometry'].centroid
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels['YRBUILT'] = [pd.to_numeric(x) for x in parcels['YRBUILT']]

# %%
parcels = parcels.sjoin(jurisdictionsPartsRaw[['PLACECOMBO', 'geometry']]).drop(columns=['index_right'])

# %%
parcels = morpcParcels.get_housing_unit_type_field(parcels, "ACRES", 'LUC')

# %%
parcels = parcels.reset_index().rename(columns = {
    'PARID':'OBJECTID',
    'LUC':'CLASS'
})
parcels['COUNTY'] = 'Fairfield'

# %%
parcels = parcels.loc[(parcels['TYPE']!='nan')&(~parcels['YRBUILT'].isna())&(~parcels['UNITS'].isna())].sort_values('UNITS', ascending=False)

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Fairfield'], fill="None", color='black')
    + plotnine.geom_jitter(parcels.loc[parcels['TYPE']!='nan'], plotnine.aes(x='x', y='y', size = 'UNITS', fill = 'TYPE'), color="None")
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
parcels[['OBJECTID', 'CLASS', 'ACRES', 'YRBUILT', 'UNITS', 'TYPE', 'COUNTY', 'PLACECOMBO', 'x', 'y', 'geometry']]
if not os.path.exists('./output_data/'):
    os.makedirs('./output_data/')
if not os.path.exists('./output_data/hu_type_from_parcels.gpkg'):
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg')
else:
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg', mode='a')

# %%
