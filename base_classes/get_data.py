import abc


class GetFromSql:
    @abc.abstractmethod
    def provide_sql_credentials(self):
        pass

    @abc.abstractmethod
    def check_connection(self):
        pass

    @abc.abstractmethod
    def return_data(self):
        pass


class GetFromNosql:
    @abc.abstractmethod
    def provide_nosql_credentials(self):
        pass

    @abc.abstractmethod
    def check_connection(self):
        pass

    @abc.abstractmethod
    def return_data(self):
        pass


class GetFromFileStorage:
    @abc.abstractmethod
    def provide_storage_credentials(self):
        pass

    @abc.abstractmethod
    def check_connection(self):
        pass

    @abc.abstractmethod
    def download_data(self):
        pass
