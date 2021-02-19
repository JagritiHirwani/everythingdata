from azure_utilities.azure_file_storage.blob.blob_send_data import BlobSendData


if __name__ == '__main__':
    blob = BlobSendData(
        connection_string="DefaultEndpointsProtocol=https;AccountName=sajalsirohi;AccountKey=XzfX8bC04B7CMwrBFLNP"
                          "/ub3uv1Pwaag8l3CNmh++GnHf4tGU9yy7EiSxbSkw73g3HzJluAyKKvorMpwJlHFlw==;EndpointSuffix=core"
                          ".windows.net"
    )
    blob.check_connection()
    blob.create_container_if_does_not_exist()
    blob.upload_data(upload_file_path="/Users/ssirohi3/Desktop/Hello.txt")