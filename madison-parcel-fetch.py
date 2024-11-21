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
morpcParcels.download_and_unzip_archive(url = "http://ftp1.co.madison.oh.us:81/Auditor/Data/GIS/", filename='parcels.zip', temp_dir='./input_data/madison_data/parcels/')

# %%
morpcParcels.download_and_unzip_archive(url='http://madison-public.issg.io/api/Document/', filename='PublicRecordsExtract.zip', temp_dir='./input_data/madison_data/cama/', keep_zip=True)

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
madison_parcels_raw = gpd.read_file('./input_data/madison_data/parcels/parcels.shp')

# %%
build = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Building.xml', columns=['Parcel_Number', 'Card', 'Year_Built', 'Year_Effective']).set_index('Parcel_Number')
dwell = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Dwelling.xml', columns=['Parcel_Number', 'Card', 'Year_Built', 'Year_Effective']).set_index('Parcel_Number')
yrbuilt = pd.concat([build, dwell]).dropna()
yrbuilt['Year_Effective'] = [x if y==None else y for x, y in zip(yrbuilt['Year_Built'], yrbuilt['Year_Effective'])]
yrbuilt = yrbuilt.groupby('Parcel_Number').agg({'Year_Effective':'max'})

# %%
units_build = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Building Composite.xml', columns=['Parcel_Number', 'Units']).set_index('Parcel_Number')
units_build['Units'] = [int(x) for x in units_build['Units']]
units_dwell = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel Dwelling.xml', columns=['Parcel_Number', 'Card', 'Units_Designed', 'Units_Converted'])
units_dwell['Units'] = [int(x) if y=='0' else int(y) for x, y in zip(units_dwell['Units_Designed'], units_dwell['Units_Converted'])]
units_dwell = units_dwell.groupby('Parcel_Number').agg({'Units':'sum'})
units = pd.concat([units_build, units_dwell]).dropna()

# %%
acres = morpcParcels.extract_fields_from_cama(zip_path='./input_data/madison_data/cama/PublicRecordsExtract.zip', filename='Parcel.xml', columns=['Parcel_Number', 'Acres']).set_index('Parcel_Number')

# %%
value = morpcParcels.extract_fields_from_cama('./input_data/madison_data/cama/PublicRecordsExtract.zip', 'Parcel Value.xml')
land_use = value[['Parcel_Number', 'Land_Use_Code']].set_index('Parcel_Number')

# %%
parcels = madison_parcels_raw[['TAXPIN', 'geometry']].set_index('TAXPIN')

# %%
parcels = parcels.join([land_use, yrbuilt, acres, units]).drop_duplicates()

# %%
parcels['geometry'] = parcels['geometry'].centroid
parcels = parcels.loc[~parcels['geometry'].isna()]

# %%
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels = parcels.sjoin(jurisdictionsPartsRaw[['PLACECOMBO', 'geometry']]).drop(columns = "index_right")

# %%
parcels = parcels.reset_index().rename(columns = {
    'TAXPIN':'OBJECTID',
    'Acres':'ACRES',
    'Land_Use_Code':'CLASS',
    'Year_Effective':'YRBUILT', 
    'Units':'UNITS'})

# %%
parcels = morpcParcels.get_housing_unit_type_field(parcels, 'ACRES', 'LUC')

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Madison'], fill="None", color='black')
    + plotnine.geom_jitter(parcels.loc[parcels['TYPE']!='nan'], plotnine.aes(x='x', y='y', size = 'UNITS', fill = 'TYPE'), color="None")
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
parcels = parcels.to_crs('3735')
parcels['COUNTY'] = 'Madison'

# %%
parcels[['OBJECTID', 'CLASS', 'ACRES', 'YRBUILT', 'UNITS', 'TYPE', 'COUNTY', 'PLACECOMBO', 'x', 'y', 'geometry']]
if not os.path.exists('./output_data/'):
    os.makedirs('./output_data/')
if not os.path.exists('./output_data/hu_type_from_parcels.gpkg'):
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg')
else:
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg', mode='a')

# %%
