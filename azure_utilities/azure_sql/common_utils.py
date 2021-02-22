from beartype import beartype
from common.sql_utilities import *


class SQLCredentials:
    """
    A class to maintain credentials that user passes, and pass them through different classes
    """
    def __init__(self,
                 server_name   = "default-server-python",
                 database_name = "default-database-python",
                 db_username   = "default-username-python",
                 db_password   = "Password1@"
                 ):
        self.db_username   = db_username
        self.db_password   = db_password
        self.server_name   = server_name
        self.database_name = database_name


def execute_raw_query(cursor, query, print_results = True, return_result = False):
    """
      Execute raw query on your db using the conn object
      :param cursor:
      :param return_result:
      :param query:
      :param print_results: Turn off printing of queries
      :return:
    """

    if print_results:
        print(f"Executing query : {query}")

    cursor.execute(query)

    if return_result:
        result = cursor.fetchall()
        return result


@beartype
def get_jdbc_url(sql_credentials: SQLCredentials) -> str:
    """
    get JDBC URL of your database
    :return:
    """
    jdbc_url = create_jdbc_url(
        host_name     = f"{sql_credentials.server_name}.database.windows.net",
        database_name = sql_credentials.database_name,
        username      = f"{sql_credentials.db_username}@{sql_credentials.server_name}",
        password      = sql_credentials.db_password
    ) + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
    print("JDBC URL: " + jdbc_url)
    return jdbc_url


def validate_sp_login():
    """
    Validate if the user has logged in using his service principal credentials and the env variables are set or not
    :return:
    """
    import os
    subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
    client_id       = os.environ.get('AZURE_CLIENT_ID')
    client_secret   = os.environ.get('AZURE_CLIENT_SECRET')
    tenant_id       = os.environ.get('AZURE_TENANT_ID')
    assert subscription_id and client_id and client_secret and tenant_id, \
        f"Please login using 'login()' method and passing your service principal credentials, as AzureSQL " \
        f"class uses SP_credentials which are exported as environment variables, and current values of those " \
        f"variables are subscription_id -> {subscription_id}, tenant_id -> {tenant_id}," \
        f"client_id -> {client_id}," \
        f"client_secret -> {client_secret}"


@beartype
def validate_table_schema_structure(schema: list) -> list :
    """
    Validate the structure of the schema
    :param schema:
    :return:
    """
    temp_schema = []
    for col_value in schema:
        assert isinstance(col_value, dict), f"All values of schema, should be a dict, {col_value} is not"
        assert len({'col_name', 'datatype'} - set(col_value.keys())) == 0, \
            f"'col_name and datatype must be present, they are not in {col_value}"

        # lower case the name of the column and store it in new temp dict.
        temp_schema.append({
            'col_name': col_value['col_name'].lower(),
            'datatype': col_value['datatype']
        })
    temp_schema.append({
        'col_name': 'create_dttm',
        'datatype': 'datetime default CONVERT(varchar,current_timestamp,20) not ',
    })
    return temp_schema