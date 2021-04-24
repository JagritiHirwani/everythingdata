from azure_utilities.Utils import create_storage_account
from azure_utilities.identity import Identity
from base_classes.send_data import SendToFileStorage

try:
    import os , uuid
    import beartype
    from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, PublicAccess
    from azure.mgmt.storage import StorageManagementClient
except Exception as err:
    print(f"Error occurred while importing blob libraries. err -> {err.args[0]}")


class BlobSendData(SendToFileStorage):
    """
    A class to send data to blob storage
    """
    def __init__(self,
                 connection_string = None,
                 container_name    = None,
                 identity          = None,
                 **options):

        self.connection_string    = connection_string
        self.container_name       = container_name or "defaultcontainerpython"
        self.blob_service_client  = BlobServiceClient.from_connection_string(self.connection_string) \
            if connection_string else None
        self.container_client     = ContainerClient.from_connection_string(self.connection_string, self.container_name)\
            if connection_string and container_name else None
        self.identity             = identity if identity and isinstance(identity, Identity) else None

    def create_container_if_does_not_exist(self, container_name = None, **options):
        """
        Create a blob container if it already does not exist
        :param container_name:
        :return:
        """
        self.container_name = container_name if container_name else self.container_name
        list_of_containers = [itr.name for itr in
                              self.blob_service_client.list_containers(name_starts_with=self.container_name)]
        if self.container_name not in list_of_containers:
            self.blob_service_client.create_container(name = self.container_name,
                                                      public_access = PublicAccess.Container)
            self.container_client = self.blob_service_client.get_container_client(container = self.container_name)

    def check_connection(self):
        """
        Check connection string is right, and the application is able to connect to the storage account
        :return:
        """
        try:
            itr = self.blob_service_client.list_containers()
            print("Able to connect to the blob container, here are all the containers")
            [print(container) for container in itr]
        except Exception as e:
            print(f"Unable to connect, error -> {e.args}")

    def create_storage_account(self, container_name = None, **options):
        """
        Create a storage container
        :param container_name:
        :param options:
        :return:
        """
        assert self.identity, "Please login to your account using identity.login() method and pass the object"
        self.container_name   = container_name or self.container_name
        meta_data             = create_storage_account(id_ = self.identity, **options)
        keys                  = meta_data.keys
        storage_account_name  = meta_data.storage_account_name
        resource_group_name   = meta_data.resource_group_name
        storage_client        = meta_data.storage_client

        print(f"Primary key for storage account: {keys.keys[0].value}")

        conn_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName=" \
                      f"{storage_account_name};AccountKey={keys.keys[0].value}"

        self.connection_string   = conn_string
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connection_string)
        self.container_client    = self.blob_service_client.create_container(self.container_name,
                                                                             public_access='container')

        container = storage_client.blob_containers.create(resource_group_name,
                                                          storage_account_name,
                                                          self.container_name, {})

        print(f"Provisioned blob container {container.name}")

    def provide_storage_credentials(self):
        pass

    def upload_data(self, upload_file_path = None, file_name = None):
        """
        Upload the file to the blob
        :param upload_file_path:
        :param file_name:
        :return:
        """
        if not file_name:
            try:
                file_name = upload_file_path.split('/')[-1]
            except:
                file_name = str(uuid.uuid4())
        # Create a blob client using the local file name as the name for the blob
        blob_client = self.blob_service_client.get_blob_client(container=self.container_name, blob=file_name)

        print("\nUploading to Azure Storage as blob:\n\t" + file_name)

        # Upload the created file
        try:
            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data)
        except Exception as e:
            print(f"Error uploading blob, err -> {e.args}")