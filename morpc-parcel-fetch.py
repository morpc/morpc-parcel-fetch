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
sys.path.append(os.path.normpath('../morpc-parcel-fetch/'))
import morpcParcels

# %%
parcel_urls = {
    "Delaware":"https://services2.arcgis.com/ziXVKVy3BiopMCCU/arcgis/rest/services/Parcel/FeatureServer/0",
    "Pickaway":"https://services6.arcgis.com/FhJ42byMw3LmPYCN/arcgis/rest/services/Parcels_Search/FeatureServer/1",
    "Logan":"https://services9.arcgis.com/mFxO7gBbusFBQ5o9/ArcGIS/rest/services/Logan_County_Parcels/FeatureServer/12",
    "Ross":"https://services7.arcgis.com/IQSUQhVBDHAkRlWe/ArcGIS/rest/services/parcel_joined/FeatureServer/0"
}

# %%
morpcParcels.gdf_from_services(url = parcel_urls['Delaware'], fieldIds=["CLASS","YRBUILT","ACRES"])

# %%
