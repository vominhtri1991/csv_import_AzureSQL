# This script using Azure AD application RBAC for gant access to Azure Blob Storage
# Need create application registration in Azure AD configure RBAC access to Storage Account fisrt
# Version 1- Tto1991
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.identity import ClientSecretCredential



tenant_id = "7d63d2f4-0426-4a5e-b25a-c4df883a18b1"
app_id= "cfe5489a-6081-4d1b-8942-b804b8e1bf73"
app_access_key = "gJt8Q~48bw0XgvkXBVwWzvJQWQbVAzTzPC6Rac0Y"

storage_uri = "https://trivmlab09.blob.core.windows.net"
container_name = "ebooks"
loca_download = "D:\MyDoc\MyCode\Azure"


azure_credential = ClientSecretCredential(
       tenant_id = tenant_id,
       client_id = app_id,
       client_secret = app_access_key,
   )


blob_service_client = BlobServiceClient(account_url=storage_uri,credential=azure_credential)
container_name = "new-ebooks"



#Create a blob client using the local file name as the name for the blob
container_client = blob_service_client.get_container_client("ebooks")
blob_list = container_client.list_blobs()
for blob in blob_list:
    print(f"File in container: {container_name}" + blob.name)