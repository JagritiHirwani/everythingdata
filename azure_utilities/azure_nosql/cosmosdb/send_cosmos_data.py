from azure_utilities.azure_nosql.cosmosdb.create_cosmosdb_instance import CreateCosmosdbInstance
from beartype import beartype


class SendCosmosData:

    def __init__(self, uri, key,
                 database_name  = "defaultpythondb",
                 container_name = "defaultpythoncontainer",
                 **options):
        self.database_name   = database_name
        self.uri             = uri
        self.key             = key
        self.container_name  = container_name
        self.create_db_instance = CreateCosmosdbInstance(
            uri=uri, key=key, database_name=database_name, container_name=container_name, **options
        )

    def create_container_if_does_not_exist(self, partition_key_path):
        """
        Create a container if it does not exist.
        :return:
        """
        self.create_db_instance.create_container_if_does_not_exists(partition_key_path = partition_key_path)

    def create_database_if_doest_not_exists(self, **options):
        """
        Create a database if it does not exists
        :return:
        """
        self.create_db_instance.create_db_if_doest_not_exists(**options)

    @beartype
    def commit_batch_data(self, data: (dict, list)):
        """
        Commit the data
        :param data:
        :return:
        """
        if isinstance(data, dict):
            data = [data]
        for datum in data:
            self.create_db_instance.container_client.upsert_item(datum)