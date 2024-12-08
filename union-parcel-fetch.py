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
# # Union County Parcels

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
parcels_raw = morpcParcels.gdf_from_services('https://www7.co.union.oh.us/unioncountyohio/rest/services/ParcelAllSaleinfoBuildingInfo/MapServer/1', crs=None)

# %%
addr_raw = morpcParcels.gdf_from_services('https://www7.co.union.oh.us/unioncountyohio/rest/services/Address/FeatureServer/0', crs=None)

# %%
parcels_raw = parcels_raw.set_crs('NAD83')

# %%
addr_raw = addr_raw.set_crs('NAD83')

# %%
parcels = parcels_raw[['ParcelNo', 'PARCELCLASS', 'yearBuilt', 'Acreage', 'geometry']].drop_duplicates()

# %%
parcels = parcels.rename(columns={
    'ParcelNo':'OBJECTID',
    'Acreage':'ACRES',
    'yearBuilt':'YRBUILT',
    'PARCELCLASS':'CLASS',
})

# %%
parcels['ACRES'] = [pd.to_numeric(x, errors='coerce') for x in parcels['ACRES']]
parcels['YRBUILT'] = [pd.to_numeric(x, errors='coerce') for x in parcels['YRBUILT']]
parcels['OBJECTID'] = [str(pd.to_numeric(x, errors='coerce')) for x in parcels['OBJECTID']]

# %%
parcels = parcels.loc[~parcels['OBJECTID'].isna()]

# %%
parcels = parcels.groupby(['OBJECTID', 'geometry']).agg({'CLASS':'first', 'ACRES':'max','YRBUILT':'max'}).drop('nan').reset_index()

# %%
parcels = gpd.GeoDataFrame(parcels, geometry='geometry')

# %%
units = parcels.sjoin(addr_raw[['AddressID', 'geometry']].to_crs(parcels.crs)).groupby('OBJECTID').agg({'AddressID':'count'}).rename(columns={'AddressID':'UNITS'})

# %%
parcels = parcels.set_index('OBJECTID').join(units).reset_index()

# %%
parcels = morpcParcels.get_housing_unit_type_field(parcels, 'ACRES', 'CLASS')

# %%
parcels = parcels.to_crs('epsg:3735')
parcels['geometry'] = parcels['geometry'].centroid
parcels = parcels.loc[~parcels['geometry'].isna()]
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels = parcels.sjoin(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=="Union"][['PLACECOMBO', 'geometry']]).drop(columns='index_right')

# %%
parcels['COUNTY'] = 'Union'

# %%
parcels = parcels.loc[(parcels['TYPE']!='nan')&(~parcels['YRBUILT'].isna())&(~parcels['UNITS'].isna())].sort_values('UNITS', ascending=False)

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Union'].to_crs(parcels.crs), fill="None", color='black')
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
