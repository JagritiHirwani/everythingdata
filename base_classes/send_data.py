import abc


class SendData(metaclass=abc.ABCMeta):
    class SendToSql(metaclass=abc.ABCMeta):

        @abc.abstractmethod
        def provide_sql_credentials(self):
            pass

        @abc.abstractmethod
        def check_connection(self):
            pass

        @abc.abstractmethod
        def create_schema(self):
            pass

        @abc.abstractmethod
        def validate_schema(self):
            pass

        @abc.abstractmethod
        def create_sql_db(self):
            pass

        @abc.abstractmethod
        def commit_data(self):
            pass

    class SendToNosql():
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

    class SendToFilestorage():
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
