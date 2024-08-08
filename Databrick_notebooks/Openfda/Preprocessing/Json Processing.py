# Databricks notebook source
# MAGIC %md
# MAGIC Creating mount to process raw json files as unirtcatalog external storage accessed via abfss protocal are not supported by python os module. os module expects local storages like mounts. Using SP to create mount.  

# COMMAND ----------

def mounting_func(container,account):
    client_id = dbutils.secrets.get(scope = 'openfdafiles_vault', key = 'client-id')
    tenant_id = dbutils.secrets.get(scope = 'openfdafiles_vault', key = 'tenet-id')
    cleient_secret = dbutils.secrets.get(scope = 'openfdafiles_vault', key = 'cleient-secret')
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":cleient_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    if any(mount.mountPoint == f"/mnt/{account}/{container}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{account}/{container}")
    dbutils.fs.mount(
                        source = f"abfss://{container}@{account}.dfs.core.windows.net/",
                        mount_point = f"/mnt/{account}/{container}",
                        extra_configs = configs)
    #display(dbutils.fs.mounts())

# COMMAND ----------

mounting_func("openfda","openfdafiles")

# COMMAND ----------

import json
import os

# No need to change the working directory; specify full paths directly
pattern = r'[a-z]+_[a-z]+_([0-9]+).json'

base_dir = '/dbfs/mnt/openfdafiles/openfda/rawdata/'
cleaned_dir_base = '/dbfs/mnt/openfdafiles/openfda/cleaneddata/'

for folder in os.listdir(base_dir):
    for subfolder in os.listdir(os.path.join(base_dir, folder)):
        for file in os.listdir(os.path.join(base_dir, folder, subfolder)):
            output_dir_path = os.path.join(cleaned_dir_base, folder, subfolder)
            output_file_path = os.path.join(output_dir_path, f"curated_{file}")
            # Ensure the directory exists before writing the file
            os.makedirs(output_dir_path, exist_ok=True)
            with open(os.path.join(base_dir, folder, subfolder, file), 'r') as l:
                with open(output_file_path, mode='w') as c: 
                    c.write(json.dumps(json.load(l)['results']))