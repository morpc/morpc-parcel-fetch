# Create a GeoDataFrame object from an ArcGIS service feature layer filtered by column names. 
def gdf_from_services(url, fieldIds = None, crs='from_service'):
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

    crs : 'from_service', None, or str to pass to gpd.GeoDataframe.set_crs()

    Returns
    ----------
    gdf : pandas.core.frame.DataFrame
        A GeoPandas GeoDataframe constructed from the GeoJSON requested from the url.
    """

    import os
    import requests
    import json
    import re
    from tqdm import tqdm
    import geopandas as gpd
    import pandas as pd
    from IPython.display import clear_output

    
    #Construct urls to query for getting total count, json, and geojson
    count_url = f"{url}/query?outFields=*&where=1%3D1&f=geojson&returnCountOnly=true"
    json_url = f"{url}/?f=pjson"
    geojson_url = f"{url}/query?outFields=*&where=1%3D1&f=geojson"

    # Request the total record count from the API
    r = requests.get(count_url)
    # Extract the JSON from the API response
    result = r.json()
    # Extract the total record count from the JSON
    totalRecordCount = int(re.findall('[0-9]+',str(r.json()))[0])

    # Request JSON and find maxRecordCount
    r = requests.get(json_url)
    result = r.json()
    maxRecordCount = result['maxRecordCount']
    
    if crs == 'from_service':
        crs = result['extent']['spatialReference']['latestWkid']
    elif crs == None:
        crs = None
    else:
        crs = crs
        
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
            gdf = pd.concat([gdf, temp], axis='index')
     
        # Increase the offset so that the next request fetches the next chunk of data
        offset += maxRecordCount
        print(f"{offset} of {totalRecordCount}")
        clear_output(wait = True)
        
    if crs != None:
        gdf = gpd.GeoDataFrame(gdf, geometry='geometry').set_crs(crs)
    else:
        gdf = gpd.GeoDataFrame(gdf, geometry='geometry')
        
    return(gdf)

# Download and unzip a file from a url. 
def download_and_unzip_archive(url, filename = None, temp_dir = "./temp_data/", keep_zip = False):
    """Creates a local copy of the contents of a zip archive from a url. 

    Parameters:
    -------------
    url | str
    The url of the directory where the zip file is located.

    filename | str
    The filename of the zip file ending in ".zip".

    temp_dir | str
    The location to create and/or to archive the files.

    keep_zip | boolean
    if True keep the zip file in the temp dir, if False deletes zip file after unarchiving

    """
    import os
    import re
    import requests
    import zipfile
    import pandas as pd
    from tqdm import tqdm
    
    # Create folder at location designated by temp_dir
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # URL for location of data
    if filename == None:
        r = requests.get(url, stream=True)
        content_dispo = str(r.headers['content-disposition'])
        if content_dispo != '':
            filename = str(re.findall('\"(.+)\"', r.headers['content-disposition'])[0])
        else:
            filename = 'no_filename.zip'
    else:
        r = requests.get(os.path.join(url, filename), stream=True)

    # Download copy of zip file from url
    archive_path = os.path.join(temp_dir, filename)
    if r.headers.get("Content-Length") != None:
        content_length = pd.to_numeric(r.headers.get("Content-Length"))
    elif r.headers.get('Transfer-Encoding') == 'chunked':
        content_length = pd.to_numeric(requests.head(os.path.join(url, filename), headers={'Accept-Encoding': None}).headers.get("Content-Length"))
    else:
        Print("No Content Length on Header")
        raise RuntimeError

    with tqdm(total=content_length) as pb:
        with open(archive_path, "wb") as fd:
            for chunk in r.iter_content(chunk_size=128):
                fd.write(chunk)
                pb.update(len(chunk))
                
    # Unzip file
    with zipfile.ZipFile(archive_path) as zip:
        for zip_info in zip.infolist():
            zip.extract(zip_info, temp_dir)

    if keep_zip == False:
        print(f"Removing zip")
        os.unlink(archive_path) # remove zip file

def extract_fields_from_cama(zip_path, filename, columns=None):
    import zipfile
    import xml.etree.ElementTree as ET
    import pandas as pd
    import random

    with zipfile.ZipFile(zip_path) as z:
        for file in z.namelist():
            if file == filename:
                file_type = file.split(".")[-1]
                with z.open(file) as data:
                    if file_type == "xml":
                        et = ET.parse(data)
                        root = et.getroot()
                        dicts = []
                        for child in root:
                            dicts.append(child.attrib)
                        if columns==None:
                            df = pd.DataFrame(dicts)
                        else:
                            df = pd.DataFrame(dicts)[columns]
                    if file_type == "csv":
                        if columns == None:
                            df = pd.read_csv(data, dtype = 'str')
                        else:
                            df = pd.read_csv(data, dtype = 'str', usecols = columns)
                    if file_type == "txt":
                        if columns == None:
                            df = pd.read_csv(data, sep = "|", dtype="str")
                        else:
                            df = pd.read_csv(data, sep = "|", dtype="str", usecols = columns)
                    if file_type == "xlsx":
                        if columns == None:
                            df = pd.read_excel(data)
                        else:
                            df = pd.read_excel(data, usecols = columns)
    return(df)

def sample_columns_from_df(df):
    import pandas as pd
    import random
    import textwrap
    column_df = pd.DataFrame(data = {"column_name":df.columns.tolist()})
    column_df['column_sample'] = ""
    for column in column_df['column_name']:
            uniques = df[column].unique().tolist()
            if len(uniques) > 10:
                k = 10
            else: 
                k = len(uniques)
            sample = "; ".join([str(x) for x in random.sample(uniques, k)])
            column_df.loc[column_df['column_name']==column, 'column_sample'] = sample
            column_df.loc[column_df['column_name']==column, 'unique_values'] = len(uniques)
            column_df.loc[column_df['column_name']==column, 'is_empty'] = f"{sum(df[column].values == ' ')} ({round((sum(df[column].values == ' ')/df.shape[0])*100)}%)"
            column_df.loc[column_df['column_name']==column, 'is_null'] = f"{sum(df[column].isnull())} ({round((sum(df[column].isnull())/df.shape[0])*100)}%)"
            column_df.loc[column_df['column_name']==column, 'is_zero_int'] = f"{sum(df[column].values == 0)} ({round((sum(df[column].values == 0)/df.shape[0])*100)}%)"
            column_df.loc[column_df['column_name']==column, 'is_zero_chr'] = f"{sum(df[column].values == '0')} ({round((sum(df[column].values == '0')/df.shape[0])*100)}%)"
    column_df = column_df.replace("0 (0%)", "")
    return(column_df)


def get_housing_unit_type_field(table, acres_name, luc_name):
    import pandas as pd
    table[acres_name] = [pd.to_numeric(x) for x in table[acres_name]]
    table[luc_name] = [str(x) for x in table[luc_name]]

    table.loc[(table[acres_name] > .75 )& (table[luc_name].str.startswith(('51', '501', '502', '503', '504', '505'))), 'TYPE'] = "SF-LL"
    table.loc[(table[acres_name] <= .75) & (table[luc_name].str.startswith(('51', '501', '502', '503', '504', '505'))), 'TYPE'] = "SF-SL"
    table.loc[table[luc_name].str.startswith(('52', '53', '54', '55')), 'TYPE'] = "SF-A"
    table.loc[table[luc_name].str.startswith(('401', '402', '403')), 'TYPE'] = "MF"
    return(table)
