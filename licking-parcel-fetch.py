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
parcels = parcels.set_index('PARCEL').join(units)

# %%
parcels = parcels.loc[(~parcels['LUC'].isna()) & (parcels['YEARBUILT']!=0)]

# %%
parcels = parcels.rename(columns={
    'PARCEL':'OBJECTID',
    'LUC':'CLASS',
    'GISACRES':'ACRES',
    'YEARBUILT':'YRBUILT'
})

# %%
parcels = morpcParcels.get_housing_unit_type_field(parcels, 'ACRES', 'CLASS')

# %%
parcels['geometry'] = parcels['geometry'].centroid
parcels = parcels.loc[~parcels['geometry'].isna()]
parcels['x'] = [point.x for point in parcels['geometry']]
parcels['y'] = [point.y for point in parcels['geometry']]

# %%
parcels = parcels.sjoin(jurisdictionsPartsRaw[['PLACECOMBO', 'geometry']]).drop(columns='index_right')

# %%
parcels['COUNTY'] = 'Licking'


# %%
parcels = parcels.reset_index().rename(columns={'PARCEL':'OBJECTID'})

# %%
parcels = parcels.loc[(parcels['TYPE']!='nan')&(~parcels['YRBUILT'].isna())&(~parcels['UNITS'].isna())].sort_values('UNITS', ascending=False)

# %%
(plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY']=='Licking'], fill="None", color='black')
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
parcels

# %%
parcels[['OBJECTID', 'CLASS', 'ACRES', 'YRBUILT', 'UNITS', 'TYPE', 'COUNTY', 'PLACECOMBO', 'x', 'y', 'geometry']]
if not os.path.exists('./output_data/'):
    os.makedirs('./output_data/')
if not os.path.exists('./output_data/hu_type_from_parcels.gpkg'):
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg')
else:
    parcels.to_file('./output_data/hu_type_from_parcels.gpkg', mode='a')

# %%
