import os
from azure.cosmos import CosmosClient, PartitionKey


class CreateCosmosdbInstance:

    def __init__(self,
                 uri = None,
                 key = None,
                 database_name  = "defaultpythondb",
                 container_name = "defaultpythoncontainer",
                 **options):
        self.uri              = uri or os.environ.get('ACCOUNT_URI')
        self.key              = key or os.environ.get('ACCOUNT_KEY')
        self.database_name    = database_name
        self.container_name   = container_name
        self.client           = CosmosClient(self.uri, credential=key)
        self.database_client  = None
        self.container_client = None
        self.options_         = options

        self.create_db_if_doest_not_exists()
        self.create_container_if_does_not_exists()

    def create_db_if_doest_not_exists(self, database_name = None, **options):
        """
        Create a DB instance
        :param database_name:
        :return:
        """
        self.database_name   = database_name if database_name else self.database_name
        self.database_client = self.client.create_database_if_not_exists(id=self.database_name, **options)

    def create_container_if_does_not_exists(self, container_name = None, partition_key_path = None, **options):
        """
        Create a container if it does not exists already
        :param container_name: name of the container
        :param partition_key_path: path of partition key in the data that you will commit.
        eg data --> {'name': 'sajal', 'last_name': 'john'}, if 'name' is going to be your
        partition key, pass partition_key_path = "/name" or "name". Partition key should be a fairly unique
        value and should not be imbalanced which will create performance issues.
        :param options:
        :return:
        """
        partition_key_path = partition_key_path or self.options_.get('partition_key_path') \
                             or options.get('partition_key_path')
        if isinstance(partition_key_path, str) and partition_key_path[0] != "/":
            partition_key_path = "/" + partition_key_path
        self.container_name   = container_name if container_name else self.container_name
        self.container_client = self.database_client\
            .create_container_if_not_exists(
                id=self.container_name,
                partition_key=PartitionKey(path=partition_key_path or self.options_.get('partition_key_path')),
                **options
        )