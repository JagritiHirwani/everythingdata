from azure.cosmosdb.table.tableservice import TableService
from azure_utilities.Utils import create_storage_account


class CreateTableInstance:

    def __init__(self, identity, **options):
        self.identity             = identity
        self.account_key          = options.get('account_key', "")
        self.storage_account_name = options.get('storage_account_name', "")
        self.table_service        = TableService(account_name=self.storage_account_name, account_key=self.account_key)\
            if self.storage_account_name and self.account_key else None
        if options.get('connection_string'):
            self.connection_string = options.get('connection_string', "")
            self.table_service     = TableService(connection_string=self.connection_string)
        self.table_name            = options.get('table_name', "defaulttablename")

    def create_storage_account(self, **options):
        """
        Create storage account
        :return:
        """
        meta_data                 = create_storage_account(id_=self.identity, **options)
        self.account_key          = meta_data.keys.keys[0].value
        self.storage_account_name = meta_data.storage_account_name

    def create_table(self, table_name = None, **options):
        """
        Create a table
        :param table_name:
        :return:
        """
        self.table_name = table_name if table_name else self.table_name
        try:
            self.table_service.create_table(table_name=self.table_name)
        except AttributeError:
            try:
                assert self.storage_account_name and self.account_key
                self.table_service = TableService(account_name=self.storage_account_name, account_key=self.account_key)
            except AssertionError:
                assert self.connection_string, "Provide at least connection_string, or" \
                                               " storage_account_name with account_key"
                self.table_service = TableService(connection_string=self.connection_string)
            self.table_service.create_table(table_name=self.table_name)
        except Exception as e:
            print(e.args[0])