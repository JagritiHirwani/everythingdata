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
    def __init__(self, connection_string):
        self.connection_string = connection_string