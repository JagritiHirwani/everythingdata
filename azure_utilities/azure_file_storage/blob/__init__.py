from azure_utilities.azure_file_storage.blob.blob_send_data import BlobSendData
from azure_utilities.azure_file_storage.blob.blob_get_data import BlobGetData


if __name__ == '__main__':
    blob = BlobSendData(
        connection_string="DefaultEndpointsProtocol=https;AccountName=sajalsirohi;AccountKey=XzfX8bC04B7CMwrBFLNP"
                          "/ub3uv1Pwaag8l3CNmh++GnHf4tGU9yy7EiSxbSkw73g3HzJluAyKKvorMpwJlHFlw==;EndpointSuffix=core"
                          ".windows.net"
    )
    bb = BlobGetData(
        connection_string="DefaultEndpointsProtocol=https;AccountName=sajalsirohi;AccountKey=XzfX8bC04B7CMwrBFLNP"
                          "/ub3uv1Pwaag8l3CNmh++GnHf4tGU9yy7EiSxbSkw73g3HzJluAyKKvorMpwJlHFlw==;EndpointSuffix=core"
                          ".windows.net"
    )
    bb.download_this_blob(blob_name = 'Hello.txt', local_dir_path="/Users/ssirohi3")
