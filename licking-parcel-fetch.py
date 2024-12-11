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
import os
import requests
import re
import geopandas as gpd
import pandas as pd
import shapely
import itables
from IPython.display import clear_output
import plotnine

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %%
url = 'https://apps.lickingcounty.gov/arcgis/rest/services/Auditor/Parcels/MapServer/0'
count_url = f"{url}/query?outFields=*&where=1%3D1&f=json&returnCountOnly=true"
json_url = f"{url}/?f=pjson"
geojson_url = f"{url}/query?outFields=*&where=1%3D1&f=json"

# %%
# Request the total record count from the API
r = requests.get(count_url)
# Extract the JSON from the API response
result = r.json()
# Extract the total record count from the JSON
totalRecordCount = int(re.findall('[0-9]+',str(r.json()))[0])

r = requests.get(json_url)
result = r.json()
maxRecordCount = result['maxRecordCount']
maxRecordCount = 1000
crs = result['extent']['spatialReference']['latestWkid']


firstTime = True
offset = 0
exceededLimit = True
while offset < totalRecordCount:
    r = requests.get(f"{geojson_url}&resultOffset={offset}&resultRecordCount={maxRecordCount}")
    result = r.json()
    
    geom = []
    for feat in result['features']:
        geom.append(shapely.geometry.Polygon(feat['geometry']['rings'][0]))
        
    attr = []
    for feat in result['features']:
        attr.append(feat['attributes'])
        
    temp = gpd.GeoDataFrame(attr, geometry=geom)
    
    if firstTime:
        # If this is the first chunk of data, create a permanent copy of the GeoDataFrame that we can append to
        gdf = temp.copy()
        firstTime = False
    else:
        # If this is not the first chunk, append to the permanent GeoDataFrame
        gdf = pd.concat([gdf, temp], axis='index')

    offset += maxRecordCount
    print(f"{offset} of {totalRecordCount}")
    clear_output(wait = True)

gdf = gdf.set_crs(str(crs))

# %%
parcels_raw = gdf
if not os.path.exists('./input_data/licking_data/parcels/'):
    os.makedirs('./input_data/licking_data/parcels/')
parcels_raw.to_file('./input_data/licking_data/parcels/licking_parcels.shp')

# %%
url = 'https://apps.lickingcounty.gov/arcgis/rest/services/Auditor/Addresses/MapServer/1'
count_url = f"{url}/query?outFields=*&where=1%3D1&f=json&returnCountOnly=true"
json_url = f"{url}/?f=pjson"
geojson_url = f"{url}/query?outFields=*&where=1%3D1&f=json"

# %%
# Request the total record count from the API
r = requests.get(count_url)
# Extract the JSON from the API response
result = r.json()
# Extract the total record count from the JSON
totalRecordCount = int(re.findall('[0-9]+',str(r.json()))[0])

r = requests.get(json_url)
result = r.json()
maxRecordCount = result['maxRecordCount']
maxRecordCount = 1000
crs = result['extent']['spatialReference']['latestWkid']


firstTime = True
offset = 0
exceededLimit = True
while offset < totalRecordCount:
    r = requests.get(f"{geojson_url}&resultOffset={offset}&resultRecordCount={maxRecordCount}")
    result = r.json()
    
    geom = gpd.GeoSeries.from_xy([sorted(x['geometry'].values())[1] for x in result['features']], [sorted(x['geometry'].values())[0] for x in result['features']])
        
    attr = []
    for feat in result['features']:
        attr.append(feat['attributes'])
        
    temp = gpd.GeoDataFrame(attr, geometry=geom, crs=crs)
    
    if firstTime:
        # If this is the first chunk of data, create a permanent copy of the GeoDataFrame that we can append to
        gdf = temp.copy()
        firstTime = False
    else:
        # If this is not the first chunk, append to the permanent GeoDataFrame
        gdf = pd.concat([gdf, temp], axis='index')

    offset += maxRecordCount
    print(f"{offset} of {totalRecordCount}")
    clear_output(wait = True)

gdf = gdf.set_crs(str(crs))

# %%
addr_raw = gdf
if not os.path.exists('./input_data/licking_data/addr/'):
    os.makedirs('./input_data/licking_data/addr/')
addr_raw.to_file('./input_data/licking_data/addr/licking_addr.shp')

# %%
parcels_raw = gpd.read_file('./input_data/licking_data/parcels/licking_parcels.shp')

# %%
addr_raw = gpd.read_file('./input_data/licking_data/addr/licking_addr.shp')

# %%
addr_raw = addr_raw.loc[addr_raw.is_valid]

# %%
parcels = parcels_raw[['PARCEL', 'LUC', 'GISACRES', 'YEARBUILT', 'geometry']]

# %%
addr = addr_raw[['LSN', 'geometry']]

# %%
units = parcels[['PARCEL', 'geometry']].sjoin(addr).groupby('PARCEL').agg({'LSN':'count'}).rename(columns={'LSN':'UNITS'})

# %%
parcels = parcels.set_index('PARCEL').join(units).reset_index()

# %%
parcels = parcels.rename(columns={
    'PARCEL':'OBJECTID',
    'LUC':'CLASS',
    'GISACRES':'ACRES',
    'YEARBUILT':'YRBUILT'
})

# %%
parcels = parcels.to_crs('epsg:3735')

# %%
parcels.to_file('./output_data/licking_parcels.gpkg')

# %%
