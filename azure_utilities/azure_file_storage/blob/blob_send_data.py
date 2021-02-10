from abc import ABC

from base_classes.send_data import SendToFileStorage

try:
    import os, uuid
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
except Exception as err:
    print(f"Error occurred while importing blob libraries. err -> {err.args[0]}")


class BlobSendData(SendToFileStorage, ABC):
    """
    A class to send data to blob storage
    """
    def __init__(self,
                 connection_string,
                 container_name = None,
                 **options):
        self.connection_string    = connection_string
        self.container_name       = container_name or "default_container_python"
        self.blob_service_client  = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client     = None

    def create_container_if_does_not_exist(self, container_name = None, **options):
        """
        Create a blob container if it already does not exist
        :param container_name:
        :return:
        """
        self.container_name = container_name if container_name else self.container_name
        list_of_containers = [itr for itr in
                              self.blob_service_client.list_containers(name_starts_with=self.container_name)]
        if self.container_name not in list_of_containers:
            self.blob_service_client.create_container(name = self.container_name,
                                                      public_access = options.get('public_access') or 'blob')
            self.container_client = self.blob_service_client.get_container_client(container = self.container_name)

    def check_connection(self):
        pass

    def create_storage_account(self):
        pass

    def provide_storage_credentials(self):
        pass

    def upload_data(self):
        pass