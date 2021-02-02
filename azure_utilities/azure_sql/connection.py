from .common_utils import *
from common.sql_utilities import create_jdbc_url, check_connection


class Connection:
    """
    Connection class to get connection object, and all related methods
    """
    def __init__(self,
                 sql_credentials,
                 **options):
        assert isinstance(sql_credentials, SQLCredentials)
        self.sql_credentials = sql_credentials
        self.conn            = None
        self.cursor          = None
        self.jdbc_url        = options.get('jdbc_url') or get_jdbc_url(self.sql_credentials)
        self.table_name      = options.get('table_name') or None
        self.schema          = []

    def check_connection(self, **options):
        """
        Check connection to your database
        :return:
        """
        print("Checking connection....")
        assert self.jdbc_url, "JDBC URL is not set or not provided"
        print(f"Using jdbc url -> {self.jdbc_url}")
        self.conn, self.cursor = check_connection(
            jdbc_url   = self.jdbc_url,
            user_cred  = {'user': self.sql_credentials.db_username, 'password': self.sql_credentials.db_password},
            **options)
        if self.conn:
            print(f"Successfully Connected :D, serverName -> {self.sql_credentials.server_name}")
        if options.get('return_result'):
            return self.conn, self.cursor

    def connect_to_table(self,
                         table_name: str,
                         **options) -> list:
        """
        Connect to table instance if you already have a table. This function will read the schema of your table,
        and prepare the 'schema' of your defined table, and use it to execute 'commit_data()' function.
        :param table_name: name of the table
        :param options:
        :return:
        """
        self.table_name = table_name
        self.jdbc_url = get_jdbc_url(self.sql_credentials)
        self.check_connection()
        result = execute_raw_query(
            self.cursor,
            f"select COLUMN_NAME, DATA_TYPE from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '{self.table_name}'",
            return_result=True
        )
        if result:
            self.schema = []
            for val in result:
                self.schema.append({
                    'col_name': val[0].lower(),
                    'datatype': val[1]
                })
        return self.schema


