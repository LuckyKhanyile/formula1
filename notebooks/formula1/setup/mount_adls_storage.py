# Databricks notebook source
storage_acc_name="4mula1dl"
client_id =dbutils.secrets.get(scope="f4mula1-scope",key="databricks-app-client-id")
tenant_id=dbutils.secrets.get(scope="f4mula1-scope",key="databricks-app-tenant-id")
client_secret=dbutils.secrets.get(scope="f4mula1-scope",key="databricks-app-client-secret")


# COMMAND ----------

configs={
    "fs.azure.account.auth.type":"OAuth",
    "fs.azure.account.oauth.provider.type":"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id":f"{client_id}",
    "fs.azure.account.oauth2.client.secret":f"{client_secret}",
    "fs.azure.account.oauth2.client.endpoint":f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

for mount in dbutils.fs.mounts():
    print(mount.mountPoint)

# COMMAND ----------

def mount_adls(container_name):
    mountPoint = f"/mnt/{storage_acc_name}/{container_name}"
    if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
        dbutils.fs.mount(
            source=f"abfss://{container_name}@{storage_acc_name}.dfs.core.windows.net",
            mount_point=mountPoint,
            extra_configs=configs)

# COMMAND ----------

def unmount_adls(container_name):
    mountPoint = f"/mnt/{storage_acc_name}/{container_name}"
    if not any(mount.mountPoint == mountPoint for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(mountPoint)

# COMMAND ----------

#unmount_adls("processed")
#unmount_adls("raw")

# COMMAND ----------

mount_adls("processed")
mount_adls("raw")
mount_adls("presentation")

# COMMAND ----------

dbutils.fs.ls("/mnt/4mula1dl")


# COMMAND ----------

dbutils.fs.mounts()