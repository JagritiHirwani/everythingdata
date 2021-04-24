import os
from abc import ABC
from multiprocessing.pool import ThreadPool

from azure.storage.blob import BlobServiceClient, ContainerClient
from beartype import beartype

from base_classes.get_data import GetFromFileStorage


class BlobGetData(GetFromFileStorage, ABC):

    def __init__(self,
                 connection_string,
                 **options):
        self.container_name       = options.get('container_name', "defaultcontainerpython")
        self.connection_string    = connection_string
        self.blob_service_client  = BlobServiceClient.from_connection_string(connection_string)
        self.container_client     = ContainerClient.from_connection_string(connection_string, self.container_name)
        self.local_dir_path       = "./"

    def list_all_blobs(self):
        """
        List all the blobs in the container
        :return:
        """
        blob_list = self.container_client.list_blobs()
        [print(b.name) for b in blob_list]
        return blob_list

    def run(self, blobs):
        # Download 10 files at a time!
        with ThreadPool(processes=int(10)) as pool:
            return pool.map(self._save_blob_locally, blobs)

    def _save_blob_locally(self, blob):
        try:
            file_name = blob.name
        except AttributeError:
            file_name = blob
        print(file_name)
        bytes_ = self.container_client.get_blob_client(file_name).download_blob().readall()

        download_file_path = os.path.join(self.local_dir_path, file_name)
        os.makedirs(os.path.dirname(download_file_path), exist_ok=True)

        with open(download_file_path, "wb") as file:
            file.write(bytes_)
        return file_name

    @beartype
    def download_all_blobs(self, local_dir_path: str):
        """
        Download all the blobs in the defined loca_dir_path
        :param local_dir_path:
        :return:
        """
        self.local_dir_path = local_dir_path
        my_blobs = self.list_all_blobs()
        result = self.run(my_blobs)
        print(result)

    @beartype
    def download_this_blob(self, local_dir_path, blob_name: str, **options):
        """
        Interact with a particular blob
        :param local_dir_path: path where you want to download this blob
        :param blob_name: Name of the blob to download
        :return:
        """
        self.local_dir_path = local_dir_path
        self._save_blob_locally(blob_name)