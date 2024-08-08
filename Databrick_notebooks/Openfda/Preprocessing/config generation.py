# Databricks notebook source
# MAGIC %md
# MAGIC Creating mount to clean up and process config files

# COMMAND ----------

def mounting_func(container, account):
    """
    Mounts an Azure Data Lake Storage Gen2 container to Databricks file system (DBFS).

    Parameters:
    container (str): The name of the Azure storage container.
    account (str): The Azure storage account name.

    Returns:
    None
    """
    # Retrieve Azure credentials from Databricks secrets
    client_id = dbutils.secrets.get(scope="openfdafiles_vault", key="client-id")
    tenant_id = dbutils.secrets.get(scope="openfdafiles_vault", key="tenet-id")
    client_secret = dbutils.secrets.get(scope="openfdafiles_vault", key="cleient-secret")

    # Define the configuration settings for mounting the Azure container
    configs = {
        "fs.azure.account.auth.type": "OAuth",
        "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        "fs.azure.account.oauth2.client.id": client_id,
        "fs.azure.account.oauth2.client.secret": client_secret,
        "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    }

    # Check if the container is already mounted, and unmount it if so
    mount_point = f"/mnt/{account}/{container}"
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(mount_point)
        
    # Mount the container to DBFS
    dbutils.fs.mount(
        source=f"abfss://{container}@{account}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs,
    )
    # Uncomment to display all current mounts
    # display(dbutils.fs.mounts())

# COMMAND ----------

# Call the mounting function to mount the specified container
mounting_func("openfda", "openfdafiles")

# COMMAND ----------

def find_urls(data, partitions=None):
    """
    Recursively find and collect URLs from a nested data structure.

    Parameters:
    data (dict or list): The nested data structure (can be a dictionary or a list).
    partitions (list): The list to collect URLs (default is None, which initializes an empty list).

    Returns:
    list: A list of URLs found in the data structure.
    """
    if partitions is None:
        partitions = []  # Initialize an empty list if no list is provided
        
    if isinstance(data, dict):
        for key, value in data.items():
            # Recursively call find_urls for dictionary values
            find_urls(value, partitions)
    elif isinstance(data, list):
        for item in data:
            # Append the "file" key value to the partitions list
            partitions.append(item["file"])
    
    return partitions

def create_dict(lst):
    """
    Create a list of dictionaries with structured information from a list of URLs.

    Parameters:
    lst (list): The list of URLs.

    Returns:
    list: A list of dictionaries with sourceBaseURL, sourceRelativeURL, sinkPath, and sinkFileName keys.
    """
    elist = []
    for item in lst:
        edict = {}  # Initialize a new dictionary
        edict["sourceBaseURL"] = "https://download.open.fda.gov/"
        # Extract the relative URL from the full URL
        edict["sourceRelativeURL"] = item[len("https://download.open.fda.gov/"):]
        
        # Split the URL to get folder, subfolder, and file details
        folder, rest = item[len("https://download.open.fda.gov/"):].split("/", 1)
        subfolder, rest = rest.split("/", 1)
        file_name = rest.replace("/", "_").replace("-", "_").replace(".zip", "")
        
        # Define the sink path and file name
        edict["sinkPath"] = f"rawdata/{folder}/{subfolder}"
        edict["sinkFileName"] = f"{folder}_{subfolder}_{file_name}"
        
        # Append the dictionary to the list
        elist.append(edict)
        
    return elist

# COMMAND ----------

import json
import os
import shutil
from datetime import datetime

# Base directory for configuration files
base_dir = '/dbfs/mnt/openfdafiles/openfda/configfiles/'

# Check if the new configuration file exists
if os.path.exists(f'{base_dir}/rawconfignew'):
    with open(f'{base_dir}/rawconfigold', 'r') as oldfile:
        with open(f'{base_dir}/rawconfignew', 'r') as newfile:
            # Load URLs from old and new configuration files
            partold = find_urls(json.load(oldfile))
            partnew = find_urls(json.load(newfile))
            
            # Determine added and deleted URLs
            part_add = list(filter(lambda x: x not in partold, partnew))
            part_delete = list(filter(lambda x: x not in partnew, partold))
            # Combine old URLs with added ones, excluding deleted ones
            partt_full = list(filter(lambda x: x not in part_delete, partold)) + part_add
            
            # Create structured dictionaries from the URL lists
            cleanedconfigfull = create_dict(partt_full)
            cleanedconfigincadd = create_dict(part_add)
            cleanedconfigincdel = create_dict(part_delete)
            
            # Write the full, incremental add, and incremental delete configurations to JSON files
            with open(f'{base_dir}/cleanedconfigfull.json', 'w') as json_file:
                json_file.write(json.dumps(cleanedconfigfull, indent=4))
                
            with open(f'{base_dir}/cleanedconfigincadd.json', 'w') as json_file:
                json_file.write(json.dumps(cleanedconfigincadd, indent=4))
                
            with open(f'{base_dir}/cleanedconfigincdel.json', 'w') as json_file:
                json_file.write(json.dumps(cleanedconfigincdel, indent=4))
                
            # Move the old configuration file to the archives
            shutil.move(f'{base_dir}/rawconfigold', f'{base_dir}/archives')
            # Rename the archived old configuration file with a timestamp
            os.rename(f'{base_dir}/archives/rawconfigold', f'{base_dir}/archives/rawconfigold_{datetime.now()}')
            # Rename the new configuration file to be the current old configuration
            os.rename(f'{base_dir}/rawconfignew', f'{base_dir}/rawconfigold')

