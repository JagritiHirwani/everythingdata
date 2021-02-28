from azure.cosmosdb.table.tableservice import TableService
from beartype import beartype
from datetime import datetime as dt, timedelta
from common.alert import Alert
import pandas as pd


def sanitize_str(val):
    return val.replace("=" , "eq")\
              .replace(">" , "gt")\
              .replace(">=", "ge")\
              .replace("<" , "lt")\
              .replace("<=", "le")\
              .replace("!=", "ne")


class GetTableData:

    def __init__(self,
                 connection_string=None,
                 storage_account_name=None,
                 account_key=None,
                 table_name=None,
                 **options):
        assert (storage_account_name and account_key) or connection_string
        self.storage_account_name = storage_account_name
        self.account_key          = account_key
        self.table_name           = table_name or "defaulttablepython"
        self.table_service        = TableService(account_name=self.storage_account_name, account_key=self.account_key) \
            if self.storage_account_name and self.account_key else None
        if connection_string:
            self.connection_string = connection_string
            self.table_service     = TableService(connection_string=self.connection_string)
        self.differential_column = options.get('differential_column') or "Timestamp"
        self.previous_diff_val   = None
        self.executed_at         = dt.utcnow()

    def get_data_on_partition_and_row_key(self, partition_key, row_key):
        """
        Get a particular entity by passing the partition_key and the row_key of the data
        :param partition_key:
        :param row_key:
        :return: an dict object, containing the data
        """
        return self.table_service.get_entity(self.table_name, partition_key, row_key)

    def get_data(self):
        """
        Get all the data from the table in a json format
        :return:
        """
        return [task for task in self.table_service.query_entities(self.table_name)]

    def get_data_with_raw_filter(self, raw_query, select=None):
        """
        get data by using the filter conditions
        :param select: return the selected attributes
        :param raw_query: raw query format
        https://docs.microsoft.com/en-us/rest/api/storageservices/Querying-Tables-and-Entities?redirectedfrom=MSDN
        :return:
        """
        if isinstance(select, list):
            select = " , ".join(select)

        return [task for task in
                self.table_service.query_entities(self.table_name, filter=raw_query, select=select)]

    @beartype
    def get_data_sql_interface(self, select: (None, str, list), where: (None, str, list)):
        """
        Parse simple SQL like syntax and convert them into table query syntax
        :param where: condition
        :param select: Column names to be selected
        :return:
        """
        select_ = None
        filter_ = None

        if isinstance(select, str):
            if select != "*":
                select_ = select

        if isinstance(select, list):
            select_ = ",".join(select)

        if isinstance(where, str):
            filter_ = sanitize_str(where)

        if isinstance(where, list):
            san_filter = [sanitize_str(f) for f in where]
            filter_    = " and ".join(san_filter)

        return self.get_data_with_raw_filter(raw_query=filter_, select = select_)

    def return_differential_data(self,
                                 differential_column = None,
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
        self.differential_column = differential_column if differential_column else self.differential_column

        if initially_fetch_data_greater_than_this and (self.previous_diff_val != initially_fetch_data_greater_than_this)\
                and (not self.previous_diff_val):
            self.previous_diff_val = initially_fetch_data_greater_than_this
        elif not self.previous_diff_val:
            self.previous_diff_val = f"{(dt.utcnow() - timedelta(minutes=1)).isoformat()[:-3]+'Z'}"

        if self.differential_column == "Timestamp":
            data = self.get_data_sql_interface(select="*",
                                               where=f"{self.differential_column} > datetime'{self.previous_diff_val}'")
        else:
            data = self.get_data_sql_interface(select="*",
                                               where=f"{self.differential_column} > {self.previous_diff_val}")

        if len(data) == 0:
            return []

        pd.set_option("display.max_rows", None, "display.max_columns", None)
        pandas_df = pd.DataFrame(data)
        pandas_df.sort_values(by = self.differential_column, inplace=True)

        if self.differential_column == "Timestamp":
            self.previous_diff_val = pandas_df[self.differential_column].iloc[-1].isoformat()[:-9] + 'Z'
        else:
            self.previous_diff_val = pandas_df[self.differential_column].iloc[-1]
        return pandas_df

    @beartype
    def set_alert_on_live_data(self, parameter_name: str, threshold: int, **options):
        """
        Set alert on data by giving parameter name and threshold
        :return:
        """
        alert = Alert(
            parameter_name=parameter_name, threshold=threshold, **options
        )
        alert.set_alert_on_live_data(
            diff_data_func = self.return_differential_data,
            **options
        )