# Create a GeoDataFrame object from an ArcGIS service feature layer filtered by column names. 
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
    from tqdm import tqdm
    
    # Create folder at location designated by temp_dir
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

    # URL for location of data
    if filename == None:
        r = requests.get(url, stream=True)
        filename = str(re.findall('\"(.+)\"', r.headers['content-disposition'])[0])
    else:
        r = requests.get(os.path.join(url, filename), stream=True)

    # Download copy of zip file from url
    archive_path = os.path.join(temp_dir, filename)
    with tqdm(total=int(r.headers.get("Content-Length"))) as pb:
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

# When looking for the correct columns in cama files it is helpful to list all the files, fields, and a sample some of the field values. 
def sample_fields_from_zipped_cama(zip_path, filename, sample = 5):
    """ Creates a Dataframe containing the names of ech field in each file and a sample of unique values from each field up to the declared number of samples. 
    
    Use this to help identify with columns from cama files are useful for join to parcel data. 

    Parameters:
    -----------
    zip_path | Path like object, or str
        The location of the zip file.

    filename | str
        the name of the file to include in the dataframe. 
        
    sample | 5 (default): int
        The number of samples to take from each field. 

    Returns:
    ---------
    df | pandas.core.frame.DataFrame
        A pandas.DataFrame object
            filename | str 
                Name of file
            column_name | str
                Name of column
            column_sample | str
                Samples of columns separated by "; "
    """
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
                        df = pd.DataFrame(dicts)
                    if file_type == "csv":
                        df = read_csv(data, dtype = 'str')
                    if file_type == "txt":
                        df = pd.read_csv(data, sep = "|", dtype="str")
                    if file_type == "xlsx":
                        df = pd.read_excel(data)
                    cama_column_df = pd.DataFrame(data = {"filename":file, "column_name":df.columns.tolist()})
                    cama_column_df['column_sample'] = ""
                    for column in cama_column_df['column_name']:
                        uniques = df[column].unique().tolist()
                        if len(uniques) > 5: 
                            k = 5 
                        else: 
                            k = len(uniques)
                        sample = "; ".join([str(x) for x in random.sample(uniques, k)])
                        cama_column_df.loc[cama_column_df['column_name']==column, 'column_sample'] = sample
    return(cama_column_df)

def extract_fields_from_cama(zip_path, filename, columns):
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
                        df = pd.DataFrame(dicts)[columns]
                    if file_type == "csv":
                        df = pd.read_csv(data, dtype = 'str', usecols = columns)
                    if file_type == "txt":
                        df = pd.read_csv(data, sep = "|", dtype="str", usecols = columns)
                    if file_type == "xlsx":
                        df = pd.read_excel(data, usecols = columns)
    return(df)