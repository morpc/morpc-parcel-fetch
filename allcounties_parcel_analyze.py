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
# # All Parcel Analysis and Visualization

# %%
import geopandas as gpd
import pandas as pd
import plotnine
import os
import pydeck as pdk
import itables

sys.path.append(os.path.normpath('../morpc-common/'))
import morpc
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %%
counties = ['franklin', 'delaware', 'licking', 'union', 'madison', 'pickaway', 'fairfield']

# %%
all_parcels = gpd.read_file('./output_data/all_parcel_data.json')

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
all_parcels['UNITS_PER_ACRE'] = all_parcels['UNITS'] / all_parcels['CALCACRES']

# %%
all_parcels['VALPERACRE'] = [x/y for x, y in zip(pd.to_numeric(all_parcels['APPRTOT'], errors='coerce'), pd.to_numeric(all_parcels['CALCACRES'], errors='coerce'))]

# %%
type_plot = (plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY'].isin([x.title() for x in counties])].to_crs('epsg:3735'), fill="ivory", color='black', size=.1)
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY'].isin([x.title() for x in counties])].dissolve(by='COUNTY').to_crs('epsg:3735'), fill="None", color='black', size=1)
    + plotnine.geom_jitter(all_parcels.loc[(all_parcels['YRBUILT']>=2020) & (all_parcels['TYPE']!='nan')], plotnine.aes(x='x', y='y', size = 'UNITS', fill = 'TYPE'), color="None")
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
   + plotnine.scale_size_radius(range=(.2,10), breaks = (1, 50, 100, 250, 500))
   + plotnine.scale_fill_brewer(type='qual', palette=2)
   + plotnine.guides(size=plotnine.guide_legend(override_aes={'color':'black'}))
    + plotnine.facet_wrap('TYPE')
)
type_plot

# %%
plotnine.ggsave(type_plot, './output_data/type_plot.png', dpi=300)

# %%
county_units = all_parcels.loc[all_parcels['YRBUILT']>=1965].groupby(['COUNTY', 'YRBUILT']).agg({'UNITS':'sum'}).reset_index()

# %%
county_units_plot = (plotnine.ggplot() +
 plotnine.geom_line(county_units, plotnine.aes(x='YRBUILT' , y='UNITS' ,color='COUNTY')))
county_units_plot

# %%
plotnine.ggsave(county_units_plot, './output_data/county_units_plot.png', dpi=300)

# %%
type_units = all_parcels.loc[all_parcels['YRBUILT']>=1965].groupby(['TYPE', 'YRBUILT']).agg({'UNITS':'sum'}).reset_index()

# %%
type_units_plot = (plotnine.ggplot() +
 plotnine.geom_line(type_units.loc[type_units['TYPE']!='nan'], plotnine.aes(x='YRBUILT' , y='UNITS' ,color='TYPE')))
type_units_plot

# %%
plotnine.ggsave(type_units_plot, './output_data/type_units_plot.png', dpi=300)

# %%
all_parcels.loc[all_parcels['YRBUILT']>=2010].groupby(['COUNTY', 'YRBUILT']).agg({'geometry':'count'}).reset_index().pivot(columns='YRBUILT', index='COUNTY')

# %%
plot_table = all_parcels.loc[(~all_parcels['UNITS_PER_ACRE'].isna()) & (all_parcels['COUNTY']=='franklin') & (all_parcels['TYPE']!='nan')]
units_per_acre_plot = (plotnine.ggplot()
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY'].isin([x.title() for x in plot_table['COUNTY']])].to_crs('epsg:3735'), fill="ivory", color='black', size=.1)
    + plotnine.geom_map(jurisdictionsPartsRaw.loc[jurisdictionsPartsRaw['COUNTY'].isin([x.title() for x in plot_table['COUNTY']])].dissolve(by='COUNTY').to_crs('epsg:3735'), fill="None", color='black', size=1)
    + plotnine.geom_map(plot_table, plotnine.aes(fill = 'UNITS_PER_ACRE'), color="None")
    + plotnine.theme(
        panel_background=plotnine.element_blank(),
        axis_text=plotnine.element_blank(),
        axis_ticks=plotnine.element_blank(),
        axis_title=plotnine.element_blank(),
        figure_size=(12,10)
    )
   + plotnine.guides(size=plotnine.guide_legend(override_aes={'color':'black'}))
)
plotnine.ggsave(units_per_acre_plot, './output_data/units_per_acre_plot.png', dpi=600)

# %%
(plotnine.ggplot() + plotnine.geom_histogram(plot_table, plotnine.aes(x = 'UNITS_PER_ACRE')))

# %%
