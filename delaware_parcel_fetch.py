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
from IPython.display import clear_output

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %% [markdown]
# # Archive Data

# %%
addr_url = "https://services2.arcgis.com//ziXVKVy3BiopMCCU//arcgis//rest//services//Address_Point//FeatureServer//0"
addr = morpcParcels.gdf_from_services(url = addr_url, crs=None).set_crs('NAD83')
if not os.path.exists('./input_data/delaware_data/addr/'):
    os.makedirs('./input_data/delaware_data/addr/')
addr.to_file("./input_data/delaware_data/addr/delaware_addr.shp")

# %%
parcels_url = "https://services2.arcgis.com//ziXVKVy3BiopMCCU//arcgis//rest//services//Parcel//FeatureServer//0"
parcels = morpcParcels.gdf_from_services(url = parcels_url, crs=None).set_crs('NAD83')
if not os.path.exists('./input_data/delaware_data/parcels/'):
    os.makedirs('./input_data/delaware_data/parcels/')
parcels.to_file("./input_data/delaware_data/parcels/delaware_parcels.shp")

# %% [markdown]
# # Read Data

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
parcels = pyogrio.read_dataframe("./input_data/delaware_data/parcels/delaware_parcels.shp")

# %%
addr = pyogrio.read_dataframe("./input_data/delaware_data/addr/delaware_addr.shp")

# %%
parcels = parcels[['OBJECTID', 'CLASS', 'YRBUILT', 'ACRES', 'geometry']].sjoin(addr[['LSN', 'geometry']]).drop(columns='index_right')

# %%
parcels['CLASS'] = [str(x) for x in parcels['CLASS']]
parcels['YRBUILT'] = [int(x) for x in parcels['YRBUILT']]
parcels['ACRES'] = [float(x) for x in parcels['ACRES']]

# %%
parcels = parcels.groupby('OBJECTID').agg({
    'CLASS':'first',
    'YRBUILT':'max',
    'ACRES':'max',
    'LSN':'count',
    'geometry':'first'
}).rename(columns = {'LSN':'UNITS'})

# %%
parcels = morpcParcels.get_housing_unit_type_field(parcels, 'ACRES', 'CLASS')

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry', crs='NAD83').to_crs('epsg:3734')

# %%
parcels['geometry'] = parcels['geometry'].centroid

# %%
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels = parcels.sjoin(jurisdictionsPartsRaw[['PLACECOMBO', 'geometry']].to_crs('3734')).drop(columns='index_right')

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Delaware'].to_crs('epsg:3734'), fill="None", color='black')
    + plotnine.geom_jitter(parcels, plotnine.aes(x='x', y='y', size = 'UNITS', fill = 'TYPE'), color="None")
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
parcels = parcels.to_crs('epsg:3735')
parcels['COUNTY'] = 'Delaware'
parcels = parcels.reset_index()

# %%
parcels.columns.values

# %%
parcels[['OBJECTID', 'CLASS', 'ACRES', 'YRBUILT', 'UNITS', 'TYPE', 'COUNTY', 'PLACECOMBO', 'x', 'y', 'geometry']]
if not os.path.exists('./output_data/'):
    os.makedirs('./output_data/')
if not os.path.exists('./output_data/hu_type_from_parcels.gpkg'):
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg')
else:
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg', mode='a')
