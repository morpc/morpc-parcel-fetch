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
# # Delaware County Parcels

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
INPUT_DIR = "./input_data"
print("Data: {0}, layer={1}".format(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, JURISDICTIONS_PARTS_FEATURECLASS_LAYER))

inputDir = os.path.normpath(INPUT_DIR)
if not os.path.exists(inputDir):
    os.makedirs(inputDir)

jurisdictionsPartsRaw = morpc.load_spatial_data(JURISDICTIONS_PARTS_FEATURECLASS_FILEPATH, layerName=JURISDICTIONS_PARTS_FEATURECLASS_LAYER, archiveDir=inputDir)

# %%
addr_url = "https://services2.arcgis.com//ziXVKVy3BiopMCCU//arcgis//rest//services//Address_Point//FeatureServer//0"

# %%
auditor_addr = morpcParcels.gdf_from_services(url = addr_url)
if not os.path.exists('./input_data/delaware_data/addr/'):
    os.makedirs('./input_data/delaware_data/addr/')
auditor_addr.to_file("./input_data/delaware_data/addr/delaware_addr.gpkg", driver='GPKG')

# %%
parcels_url = "https://services2.arcgis.com//ziXVKVy3BiopMCCU//arcgis//rest//services//Parcel//FeatureServer//0"

# %%
delaware_parcels = morpcParcels.gdf_from_services(url = parcels_url)
if not os.path.exists('./input_data/delaware_data/parcels/'):
    os.makedirs('./input_data/delaware_data/parcels/')
delaware_parcels.to_file("./input_data/delaware_data/parcels/delaware_parcels.gpkg", driver='GPKG')

# %%
parcels = pyogrio.read_dataframe("C:\\Users\\jinskeep\OneDrive - Mid-Ohio Regional Planning Commission\\Desktop\\parcels\\parcels.shp")

# %%
units = pd.DataFrame(auditor_addr[['PARCEL_NO', 'LSN']].groupby('PARCEL_NO').size()).rename(columns = {0:'UNITS'})

# %%
parcels = parcels[['PARCEL_NO', 'CLASS', 'YRBUILT', 'ACRES', 'geometry']].set_index('PARCEL_NO').join(units)

# %%
parcels = parcels.loc[(~parcels['UNITS'].isna()) & (parcels['YRBUILT']>0)].drop_duplicates()

# %%
parcels.loc[(parcels['ACRES'] > .75 )& (parcels['CLASS'].str.startswith(('51', '501', '502', '503', '504', '505'))), 'TYPE'] = "SF-LL"
parcels.loc[(parcels['ACRES'] <= .75) & (parcels['CLASS'].str.startswith(('51', '501', '502', '503', '504', '505'))), 'TYPE'] = "SF-SL"
parcels.loc[parcels['CLASS'].str.startswith(('52', '53', '54', '55')), 'TYPE'] = "SF-A"
parcels.loc[parcels['CLASS'].str.startswith('4'), 'TYPE'] = "MF"

# %%
parcels = parcels.loc[parcels['TYPE']!='nan']

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry').to_crs('epsg:3735')

# %%
parcels['geometry'] = parcels['geometry'].centroid

# %%
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels['YRBUILT'] = [pd.to_numeric(x) for x in parcels['YRBUILT']]

# %%
parcels = parcels.sjoin(jurisdictionsPartsRaw[['PLACECOMBO', 'geometry']])

# %%
parcels = parcels.drop(columns=['index_right'])

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Delaware'], fill="None", color='black')
    + plotnine.geom_jitter(parcels.loc[parcels['YRBUILT']>2020], plotnine.aes(x='x', y='y', size = 'UNITS', fill = 'TYPE'), color="None")
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
pd.DataFrame(parcels.loc[parcels['YRBUILT']>2020].groupby(['YRBUILT', 'TYPE']).size()).rename(columns={0:'UNITS'})

# %%
pd.DataFrame(parcels.loc[parcels['YRBUILT']>2020].groupby(['PLACECOMBO', 'YRBUILT']).agg({'UNITS':'sum'})).rename(columns={0:'UNITS'}).reset_index().pivot(index='PLACECOMBO', columns='YRBUILT',values= 'UNITS')

# %%
