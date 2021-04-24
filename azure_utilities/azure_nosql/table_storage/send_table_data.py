from azure.cosmosdb.table.tableservice import TableService
from beartype import beartype

from .create_table_instance import CreateTableInstance


class SendTableData:

    def __init__(self,
                 connection_string    = None,
                 storage_account_name = None,
                 account_key          = None,
                 identity             = None,
                 table_name           = None):
        assert (storage_account_name and account_key) or connection_string
        self.storage_account_name  = storage_account_name
        self.account_key           = account_key
        self.table_name            = table_name or "defaulttablepython"

        self.create_table_instance = CreateTableInstance(
            identity             = identity,
            connection_string    = connection_string,
            storage_account_name = storage_account_name,
            account_key          = account_key
        ) if identity else None

        self.table_service = TableService(account_name=self.storage_account_name, account_key=self.account_key) \
            if self.storage_account_name and self.account_key else None
        if connection_string:
            self.connection_string = connection_string
            self.table_service     = TableService(connection_string=self.connection_string)

        self.ROW_KEY_GEN       = False
        self.PARTITION_KEY_GEN = False

    def create_table(self, table_name = None, **options):
        """
        Create a table
        :param table_name:
        :param options:
        :return:
        """
        self.table_name = table_name if table_name else self.table_name
        assert self.create_table_instance, "Initialize this obj by sending identity object in constructor"
        self.create_table_instance.create_table(table_name = self.table_name, **options)

    def create_storage_account(self, **options):
        """
        Create a storage account
        :return:
        """
        self.create_table_instance.create_storage_account(**options)

    @beartype
    def commit_data(self, data: dict):
        """
        Send data to the table. It will insert the data, and if it already exists, it will replace the data
        :param data:
        :return:
        """
        if self.ROW_KEY_GEN and self.PARTITION_KEY_GEN:
            import uuid, random
            data['RowKey']       = str(uuid.uuid4())
            data['PartitionKey'] = str(random.randint(0, 10))
        self.table_service.insert_or_replace_entity(self.table_name, data)

    @beartype
    def commit_batch_data(self, data: list):
        """
        Send data in a batch
        :param data:
        :return:
        """
        import uuid, random
        if self.ROW_KEY_GEN and self.PARTITION_KEY_GEN:
            partition_key = str(random.randint(0, 10))
            for data_ in data:
                data_['RowKey']       = str(uuid.uuid4())
                data_['PartitionKey'] = partition_key
        with self.table_service.batch(self.table_name) as batch:
            for data_ in data:
                batch.insert_entity(data_)