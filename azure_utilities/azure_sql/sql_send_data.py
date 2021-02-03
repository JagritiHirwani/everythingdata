import os
from base_classes.send_data import SendToSql
from .create_sql_instance import CreateSQLInstance
from .connection import Connection
from .common_utils import *

try:
    from azure.common.client_factory import get_client_from_cli_profile
    from azure.common.credentials import ServicePrincipalCredentials
    from azure.mgmt.resource import ResourceManagementClient
    from azure.mgmt.sql import SqlManagementClient
    from azure.mgmt.sql.models import Sku
    from beartype import beartype
except ImportError:
    raise ImportError("""Use -> pip install azure-common
                                pip install azure-mgmt-sql
                                pip install azure-mgmt-resource
                                pip install beartype 
                                to use sql with azure """)


class SQLSendData(SendToSql):
    """
    All methods related to SQL in azure are defined here
    Most of the methods makes use of this guide
    https://docs.microsoft.com/en-us/samples/azure-samples/sql-database-python-manage/sql-database-python-manage/
    """

    def __init__(self,
                 server_name   ,
                 database_name ,
                 db_username   ,
                 db_password   ,
                 **options):
        """

        :param sql_server: Server name
        :param database_name: Name of the database
        :param db_username: username of the database
        :param db_password: password of the user of the database
        :param options:
        """
        self.sql_credentials = SQLCredentials(
            server_name   = server_name    ,
            database_name = database_name  ,
            db_username   = db_username    ,
            db_password   = db_password
        )
        self.create_sql_instance  = CreateSQLInstance(self.sql_credentials, **options)
        self.connection      = Connection(self.sql_credentials)
        self.conn            = None
        self.cursor          = None
        self.schema          = options.get('schema') or []
        self.table_name      = options.get('table_name') or 'default_table_python'
        
        validate_sp_login()

    def provide_sql_credentials(self,
                                server_name    = None,
                                database_name  = None,
                                db_username    = None,
                                db_password    = None,
                                **options):
        """
        Either provide credentials using this method, or while initializing the class
        :param db_password:
        :param db_username:
        :param server_name:
        :param database_name:
        :return:
        """
        self.sql_credentials = SQLCredentials(
            server_name   = server_name    ,
            database_name = database_name  ,
            db_username   = db_username    ,
            db_password   = db_password
        )

    def check_connection(self, **options):
        """
        Check connection to your database
        :return:
        """
        self.conn, self.cursor = self.connection.check_connection(return_result=True, **options)

    @beartype
    def create_table_schema(self, schema_list: list, **options):
        """
        Create the schema, or assign the schema using this function. This schema will always be validated against
        the data that you will be sending to database. This can be ignored if not needed
        :param schema_list:
        :param options:
        :return:
        """
        self.schema = validate_table_schema_structure(schema_list)

    def validate_table_schema(self):
        pass

    @beartype
    def create_table_using_schema(self, table_name: str, **options):
        """
        Create a table, using the schema. If you want to create a table without using schema, use self.cursor to Execute
        custom query and create table. This method takes schema, dynamically creates SQL for creating a new table.
        :param table_name: Name of the table you want to create
        :param options:
        :return:
        """
        print(f"Creating table {table_name}...")
        assert self.conn and self.cursor, "Use check_connection() method to set 'conn' and 'cursor' object"
        assert self.schema, "Please define a schema to use this method using create_schema() method"
        self.table_name = table_name
        create_table_query = f"CREATE TABLE {table_name.upper()} ("
        for col in self.schema:
            try:
                create_table_query += f'[{col["col_name"]}] {col["datatype"]} null, '
            except AttributeError as err:
                print(f"Error in data, col value -> {col}")
                raise AttributeError(err.args[0])
        create_table_query = create_table_query[0:-2] + ")"
        print(f"Create table query : {create_table_query}")
        self.cursor.execute(create_table_query)
        print("Table created.")

    def create_sql_db(self, **options):
        """
        Create a SQL DB in azure based on the details used to initialize the class
        :return:
        """
        self.create_sql_instance.create_sql_db(**options)

    def create_sql_server_instance(self, **options):
        """
        Create SQL server instance
        :return:
        """
        self.create_sql_instance.create_sql_server_instance(**options)

    def create_firewall_rule(self, **options):
        """
        Firewall rule is required to enable IP addresses to be able to connect to SQL
        If starting and end IP is not given, current external IP will be used to set the rule.
        :param options:
        :param starting_ip: IP range on which the access is allowed
        :param ending_ip: Up to IP for which access is allowed
        :return:
        """
        self.create_sql_instance.create_firewall_rule(**options)

    @beartype
    def connect_to_table(self,
                         table_name  : str,
                         **options):
        """
        Connect to table instance if you already have a table. This function will read the schema of your table,
        and prepare the 'schema' of your defined table, and use it to execute 'commit_data()' function.
        :param table_name: name of the table
        :param options:
        :return:
        """
        self.schema = self.connection.connect_to_table(table_name=table_name, **options)

    @beartype
    def commit_data(self, data: (list, dict), **options):
        """
        Send the data to the DB that you have initialized. self.schema is necessary to be defined for this function.
        :param data: Data can be either of the type dict, or list. If data is list, all values should be provided
                     for the columns of the table, else it will raise SQL error.
                     If do not want to give values for all the columns, use dict, where an element looks like
                     [{ 'col_name' : <col-name>, 'value' : <value> }], and it will create query for only those cols
                     or a dict can be passed {'col_name_1': 'value_1', 'col_name_2': 'value_2'....}
        :return:
        """
        assert self.schema, "Please provide a schema by using create_table_schema() method"
        query = f"INSERT INTO {self.table_name} ("
        if isinstance(data, dict):
            for col_name in data.keys():
                query += f"{col_name}, "
            query = query[0:-2] + ") VALUES ("
            for values in data.values():
                query += f"'{values}', "
            query = query[0:-2] + ")"
            execute_raw_query(self.cursor, query)

        elif isinstance(data[0], dict):
            for col in data:
                query += f"{col['col_name']}, "
            query = query[0:-2] + ") VALUES ("
            for col in data:
                query += f"'{col['value']}', "
            query = query[0:-2] + ")"
            execute_raw_query(self.cursor, query)

        else:
            assert len(self.schema) == len(data), "All columns values are not given, as number of elements in 'data'" \
                                                   "is not equal to number of cols defined in _schema"
            for col in self.schema:
                query += f"{col['col_name']}, "
            query = query[0:-2] + ") VALUES ("
            for _data in data:
                query += f"'{_data}', "
            query = query[0:-2] + ")"
            execute_raw_query(self.cursor, query)