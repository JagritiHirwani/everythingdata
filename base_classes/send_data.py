import abc


class SendToSql(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    def provide_sql_credentials(self):
        pass

    @abc.abstractmethod
    def check_connection(self):
        pass

    @abc.abstractmethod
    def create_table_schema(self):
        pass

    @abc.abstractmethod
    def validate_table_schema(self):
        pass

    @abc.abstractmethod
    def create_sql_db(self):
        pass

    @abc.abstractmethod
    def commit_data(self):
        pass


class SendToNosql(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def provide_nosql_credentials(self):
        pass

    @abc.abstractmethod
    def check_connection(self):
        pass

    @abc.abstractmethod
    def create_nosql_db(self):
        pass

    @abc.abstractmethod
    def commit_data(self):
        pass


class SendToFileStorage(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def provide_storage_credentials(self):
        pass

    @abc.abstractmethod
    def check_connection(self):
        pass

    @abc.abstractmethod
    def create_storage_account(self):
        pass

    @abc.abstractmethod
    def upload_data(self):
        pass
