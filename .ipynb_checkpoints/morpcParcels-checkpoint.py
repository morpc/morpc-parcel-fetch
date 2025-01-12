def gdf_from_services(url, fieldIds = None):
    """Creates a GeoDataFrame from a request to an ArcGIS Services. Automatically queries for maxRecordCount and iterates
    over the whole feature layer to return all features. Optional: Filter the results by including a list of field IDs.

    Example Usage:

    Parameters:
    ------------
    url : str
        A path to a ArcGIS Service feature layer. 
        Example: https://services2.arcgis.com/ziXVKVy3BiopMCCU/arcgis/rest/services/Parcel/FeatureServer/0

    fieldIds : list of str
        A list of strings that match field ids in the feature layer.

    Returns
    ----------
    gdf : pandas.core.frame.DataFrame
        A GeoPandas GeoDataframe constructed from the GeoJSON requested from the url.
    """

    import os
    import requests
    import json
    from tqdm import tqdm
    import geopandas as gpd
    import pandas as pd

    
    #Construct urls to query for getting total count, json, and geojson
    count_url = f"{url}/query?outFields=*&where=1%3D1&f=geojson&returnCountOnly=true"
    json_url = f"{url}/?f=pjson"
    geojson_url = f"{url}/query?outFields=*&where=1%3D1&f=geojson"

    # Request the total record count from the API
    r = requests.get(count_url)
    # Extract the JSON from the API response
    result = r.json()
    # Extract the total record count from the JSON
    totalRecordCount = result['properties']["count"]

    # Request JSON and find maxRecordCount
    r = requests.get(json_url)
    result = r.json()
    maxRecordCount = result['maxRecordCount']

    avail_fields = [dict['name'] for dict in result['fields']]

    if fieldIds != None:
        if not set(fieldIds).issubset(avail_fields):
            print(f"{fieldIds} not in available fields.")
            raise RuntimeError
        else:
            outFields = ",".join(fieldIds)
            geojson_url = f"{url}/query?outFields={outFields}&where=1%3D1&f=geojson"

    firstTime = True
    offset = 0
    exceededLimit = True
    print(geojson_url)
    with tqdm(total = totalRecordCount, desc = f"Downloading from GeoJSON...") as pb:
        while offset < totalRecordCount:
            # print("Downloading records from {}&resultOffset={}&resultRecordCount={}".format(url, offset, recordCount))
            # Request 2000 records from the API starting with the record indicated by offset. 
            # Configure the request to include only the required fields, to project the geometries to 
            # Ohio State Plane South coordinate reference system (EPSG:3735), and to format the data as GeoJSON
            r = requests.get(f"{geojson_url}&resultOffset={offset}&resultRecordCount={maxRecordCount}")
            # Extract the GeoJSON from the API response
            result = r.json()
         
            # Read this chunk of data into a GeoDataFrame
            temp = gpd.GeoDataFrame.from_features(result["features"])
            if firstTime:
                # If this is the first chunk of data, create a permanent copy of the GeoDataFrame that we can append to
                gdf = temp.copy()
                firstTime = False
            else:
                # If this is not the first chunk, append to the permanent GeoDataFrame
                gdf = pd.concat([gdf, temp], axis="index")
         
            # Increase the offset so that the next request fetches the next chunk of data
            offset += maxRecordCount
            pb.update(maxRecordCount)

    return(gdf)