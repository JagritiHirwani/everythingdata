from beartype import beartype

from azure_utilities.azure_nosql.cosmosdb.create_cosmosdb_instance import CreateCosmosdbInstance
import calendar
import time
import pandas as pd

from common.alert import Alert


class GetCosmosData:

    def __init__(self, uri, key,
                 database_name="defaultpythondb",
                 container_name="defaultpythoncontainer",
                 **options):
        self.database_name  = database_name
        self.uri            = uri
        self.key            = key
        self.container_name = container_name
        self.create_db_instance = CreateCosmosdbInstance(
            uri=uri, key=key, database_name=database_name, container_name=container_name, **options
        )
        self.previous_diff_val = options.get('initially_fetch_value_greater_than_this') or calendar.timegm(time.gmtime())

    def get_data_with_query(self, sql_query, **options):
        """
        Get data using SQL Query
        :param sql_query:
        :return:
        """
        data = self.create_db_instance.container_client.query_items(
            query = sql_query, enable_cross_partition_query=True, **options
        )
        data_ = [datum for datum in data]
        try:
            data_.sort(key = lambda datum: datum['_ts'])
        except Exception as e:
            print(e.args[0])
            return data_
        return data_

    def get_all_data(self):
        """
        Get all the data from the container
        :return:
        """
        return self.get_data_with_query(sql_query=f"select * from {self.container_name}")

    def return_differential_data(self, **options):
        """
        Get differential data, currently supports only _ts column name, if not, then use table storage instead.
        :return:
        """
        data = self.get_data_with_query(sql_query=f"select * from {self.container_name} "
                                                  f"where {self.container_name}._ts > {self.previous_diff_val}")
        if data:
            pandas_df = pd.DataFrame(data)
            pandas_df.sort_values('_ts', inplace=True)
            self.previous_diff_val = pandas_df['_ts'].iloc[-1]
        else:
            pandas_df = []
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
            diff_data_func=self.return_differential_data,
            **options
        )