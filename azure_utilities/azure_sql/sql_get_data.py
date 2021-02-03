from abc import ABC
from base_classes.get_data import GetFromSql
from .connection import Connection
from .common_utils import *
from datetime import datetime as dt


class SQLGetData(GetFromSql, ABC):
    """
    Class to retrieve data from your database. You need to have your database up and running to use this class.
    """

    def __init__(self,
                 server_name,
                 database_name,
                 db_username,
                 db_password,
                 **options):
        """

        :param sql_server: Server name
        :param database_name: Name of the database
        :param db_username: username of the database
        :param db_password: password of the user of the database
        :param options:
        """
        self.sql_credentials = SQLCredentials(
            server_name    = server_name,
            database_name  = database_name,
            db_username    = db_username,
            db_password    = db_password
        )
        self.connection = Connection(self.sql_credentials)
        self.conn       = None
        self.cursor     = None
        self.schema     = options.get('schema') or []
        self.table_name = options.get('table_name') or "default_table_python"
        self.previous_diff_val = None

    def check_connection(self, **options):
        """
        Check connection to your database
        :return:
        """
        self.conn, self.cursor = self.connection.check_connection(return_result=True, **options)

    def connect_to_table(self, table_name, **options):
        """
        Connect to your table and get the schema of your table
        :param table_name:
        :param options:
        :return:
        """
        self.table_name = table_name
        self.schema, self.cursor, self.conn = self.connection.connect_to_table(table_name, **options)

    def execute_raw_query(self, query, **options):
        """
        Execute Raw query
        :return:
        """
        assert self.conn and self.cursor, "Please set these object by using check_connection() method"
        return execute_raw_query(self.cursor, query, **options)

    @beartype
    def create_table_schema(self, schema_list: list, **options):
        """
        Create the schema, or assign the schema using this function. This schema will always be validated against
        the data that you will be sending to database. This can be ignored if not needed
        :param schema_list:
        :param options:
        :return:
        """
        self.schema = validate_table_schema_structure(schema_list, **options)

    def provide_sql_credentials(self):
        pass

    def return_data(self, return_as = None, **options):
        """
        Get the whole data of the table
        :param return_as : pandas_df
        :return:
        """
        data = self.execute_raw_query(f"select * from {self.table_name}", return_result=True)
        if not return_as:
            return data
        elif return_as == 'pandas_df':
            import pandas as pd
            assert self.schema, "Provide the schema of your table or use connect_to_table() method to get the " \
                                "schema of your table."
            columns = [val['col_name'] for val in self.schema]
            df = pd.DataFrame(data, columns=columns)
            return df

    def return_differential_data(self,
                                 differential_column = "create_dttm",
                                 initially_fetch_data_greater_than_this = None,
                                 **options):
        """
        Get incremental data on the specified column. This function will fetch data which inserted after
        the self.previous_diff_val when the function was previously called.

        :param differential_column: column on which the differential parameter will be implemented
        :param initially_fetch_data_greater_than_this: initial value to be fetched
        :param options:
        :return:
        """
        if initially_fetch_data_greater_than_this and (self.previous_diff_val != initially_fetch_data_greater_than_this)\
                and (not self.previous_diff_val):
            self.previous_diff_val = initially_fetch_data_greater_than_this
        data = self.execute_raw_query(f"select * from {self.table_name} where "
                                      f"{differential_column} > '{self.previous_diff_val}'",
                                      return_result=True)
        if len(data) == 0:
            return []
        import pandas as pd
        assert self.schema, "Provide the schema of your table or use connect_to_table() method to get the " \
                            "schema of your table."
        columns = [val['col_name'] for val in self.schema]
        df = pd.DataFrame(data, columns=columns)
        self.previous_diff_val = df[differential_column].iloc[-1]
        return df