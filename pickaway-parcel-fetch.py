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
# # Pickaway County Parcel Data

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
INPUT_DIR = "./input_data/"
inputDir = os.path.normpath(INPUT_DIR)
if not os.path.exists(inputDir):
    os.makedirs(inputDir)
jurisdictionsPartsRaw = morpc.load_spatial_data(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, layerName=JURISDICTIONS_PARTS_FEATURECLASS_LAYER, archiveDir=inputDir)

# %%
parcels_raw = morpcParcels.gdf_from_services('https://services6.arcgis.com/FhJ42byMw3LmPYCN/arcgis/rest/services/parcel_joined/FeatureServer/0', crs=None)

# %%
parcels_raw = parcels_raw.set_crs('NAD83')

# %%
addr_raw = morpcParcels.gdf_from_services('https://services6.arcgis.com/FhJ42byMw3LmPYCN/ArcGIS/rest/services/Addresses/FeatureServer/0', crs=None)

# %%
addr_raw = addr_raw.set_crs('NAD83')

# %%
parcels = parcels_raw[['Parcel', 'PPYearBuilt', 'PPClassNumber', 'PPAcres', 'geometry']]

# %%
parcels = parcels.reset_index().drop(columns='index')

# %%
parcels['PPYearBuilt'] = [x.split('|')[-1] if x!=None else None for x in parcels['PPYearBuilt']]

# %%
parcels = parcels.rename(columns={
    'Parcel':'OBJECTID',
    'PPAcres':'ACRES',
    'PPYearBuilt':'YRBUILT',
    'PPClassNumber':'CLASS',
})

# %%
units = parcels.sjoin(addr_raw[['LSN', 'geometry']].to_crs(parcels.crs)).groupby('OBJECTID').agg({'LSN':'count'}).rename(columns={'LSN':'UNITS'})

# %%
parcels = parcels.set_index('OBJECTID').join(units).reset_index()

# %%
parcels = parcels.loc[(parcels['ACRES']!='nan') & (~parcels['CLASS'].isna())]

# %%
parcels = morpcParcels.get_housing_unit_type_field(parcels, 'ACRES', 'CLASS')

# %%
parcels = parcels.to_crs('epsg:3735')
parcels['geometry'] = parcels['geometry'].centroid
parcels = parcels.loc[~parcels['geometry'].isna()]
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels = parcels.sjoin(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=="Pickaway"][['PLACECOMBO', 'geometry']]).drop(columns='index_right')

# %%
parcels['COUNTY'] = 'Pickaway'

# %%
parcels = parcels.loc[(parcels['TYPE']!='nan')&(~parcels['YRBUILT'].isna())&(~parcels['UNITS'].isna())].sort_values('UNITS', ascending=False)

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Pickaway'].to_crs(parcels.crs), fill="None", color='black')
    + plotnine.geom_jitter(parcels.loc[parcels['TYPE']!='nan'], plotnine.aes(x='x', y='y', size = 'UNITS', fill = 'TYPE'), color="None")
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
parcels[['OBJECTID', 'CLASS', 'ACRES', 'YRBUILT', 'UNITS', 'TYPE', 'COUNTY', 'PLACECOMBO', 'x', 'y', 'geometry']]
if not os.path.exists('./output_data/'):
    os.makedirs('./output_data/')
if not os.path.exists('./output_data/hu_type_from_parcels.gpkg'):
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg')
else:
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg', mode='a')

# %%
parcels

# %%
