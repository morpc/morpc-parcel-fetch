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

# %%
import geopandas as gpd
import pandas as pd
import os
import plotnine
import itables
import pydeck as pdk


sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %% [markdown]
# ## Load all county data

# %%
counties = ['franklin', 'delaware', 'licking', 'union', 'madison', 'pickaway', 'fairfield']

# %%
all_parcels = []
for county in counties:
    file = gpd.read_file(f'./output_data/{county}_parcels.gpkg')
    file['COUNTY'] = county
    file.to_crs('epsg:3735')
    all_parcels.append(file)
all_parcels = pd.concat(all_parcels)

# %%
all_parcels['CALCACRES'] = all_parcels.to_crs('epsg:9822')['geometry'].area * 0.0002471054

# %%
all_parcels = morpcParcels.get_housing_unit_type_field(all_parcels, 'CALCACRES', 'CLASS')

# %%
all_parcels = all_parcels.loc[~all_parcels['geometry'].isna()]

# %%
all_parcels['centroid'] = all_parcels['geometry'].centroid

# %%
all_parcels['x'] = [point.x for point in all_parcels['centroid']]
all_parcels['y'] = [point.y for point in all_parcels['centroid']]

# %%
all_parcels['YRBUILT'] = [pd.to_numeric(x,errors='coerce') for x in all_parcels['YRBUILT']]

# %%
all_parcels.loc[(all_parcels['TYPE']=='SF-SL'), 'UNITS'] = 1
all_parcels.loc[(all_parcels['TYPE']=='SF-LL'), 'UNITS'] = 1

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
all_parcels = gpd.GeoDataFrame(all_parcels, geometry='centroid').sjoin(jurisdictionsPartsRaw[['PLACECOMBO', 'geometry']])

# %%
all_parcels = gpd.GeoDataFrame(all_parcels, geometry='geometry')

# %%
all_parcels.drop(columns='centroid').to_file('./output_data/all_parcel_data.json', driver='GeoJSON')
