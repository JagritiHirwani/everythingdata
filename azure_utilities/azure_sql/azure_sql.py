import os
from base_classes.send_data import SendToSql
from common.sql_utilities import create_jdbc_url, check_connection

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
                                pip install azure-mgmt-resource to use sql with azure """)


class AzureSQL(SendToSql):
    """
    All methods related to SQL in azure are defined here
    Most of the methods makes use of this guide
    https://docs.microsoft.com/en-us/samples/azure-samples/sql-database-python-manage/sql-database-python-manage/
    """

    def __init__(self,
                 sql_server_name = None,
                 database_name   = None,
                 username        = None,
                 password        = None,
                 **options):
        """

        :param sql_server:
        :param database_name:
        :param username:
        :param password:
        :param options:
        """
        self.sql_server_name = sql_server_name or "test-server"
        self.database_name   = database_name
        self.username        = username
        self.password        = password
        self.subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
        self.client_id       = os.environ.get('AZURE_CLIENT_ID')
        self.client_secret   = os.environ.get('AZURE_CLIENT_SECRET')
        self.tenant_id       = os.environ.get('AZURE_TENANT_ID')
        self.default_RG      = options.get('resource_group_name') or "Test-SQL-RG"
        self.region          = options.get('region') or "westus"
        self.sku             = options.get('sku') or Sku(name='Free')
        self.sql_client      = None
        self.resource_client = None
        self.db_object       = None
        self.jdbc_url        = None
        self.conn            = None
        self.cursor          = None
        self._schema         = options.get('schema') or []
        self.table           = options.get('table') or 'test-table'

        self.validate_sp_login()

        credentials = ServicePrincipalCredentials(
            client_id   = self.client_id,
            secret      = self.client_secret,
            tenant      = self.tenant_id
        )
        self.resource_client = ResourceManagementClient(credentials, self.subscription_id)
        self.sql_client      = SqlManagementClient(credentials, self.subscription_id)

    def provide_sql_credentials(self,
                                sql_server_name  = None,
                                database_name    = None,
                                username         = None,
                                password         = None,
                                **options):
        """
        Either provide credentials using this method, or while initializing the class
        :param sql_server_name:
        :param database_name:
        :param username:
        :param password:
        :return:
        """
        self.sql_server_name = sql_server_name
        self.database_name   = database_name
        self.username        = username
        self.password        = password

    def check_connection(self, **options):
        """
        Check connection to your database
        :return:
        """
        print("Checking connection....")
        assert self.jdbc_url or options.get('jdbc_url'), "JDBC URL is not set or not provided"
        self.conn, self.cursor = check_connection(
            jdbc_url  = self.jdbc_url or options.get('jdbc_url'),
            user_cred = {'user': self.username, 'password': self.password},
            **options)
        if self.conn:
            print(f"Successfully Connected :D, serverName -> {self.sql_server_name}")
        if options.get('return_result'):
            return self.conn

    @beartype
    def create_table_schema(self, schema_list: list, **options):
        """
        Create the schema, or assign the schema using this function. This schema will always be validated against
        the data that you will be sending to database. This can be ignored if not needed
        :param schema_list:
        :param options:
        :return:
        """
        self._validate_table_schema_structure(schema_list)

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
        assert self.conn and self.cursor, "Use set_connection_object() method to set 'conn' and 'cursor' object"
        assert self._schema, "Please define a schema to use this method using create_schema() method"
        self.table = table_name
        create_table_query = f"CREATE TABLE {table_name.upper()} ("
        for col in self._schema:
            try:
                print(col)
                create_table_query += f'[{col["col_name"]}] {col["datatype"]} null, '
            except AttributeError as err:
                print(f"Error in data, col value -> {col}")
                raise AttributeError(err.args[0])
        create_table_query = create_table_query[0:-2] + ")"
        print(f"Create table query : {create_table_query}")
        self.cursor.execute(create_table_query)
        print("Table created.")

    @beartype
    def create_sql_db(self, database_name: str, **options):
        """
        Create a SQL DB in azure. If server name is not given, program will throw an error, else pass optional params
        of create_new_server=True and server_name=<Server-name> or call create_new_server method.
        Provide username and password either through the params, or "provide_sql_credentials" method
        and passing neccessary detail.

        optional params:
                resource_group_params: Parameters for creating or updating a resource group
                resource_group_name: Name of the resource group, default "Test-SQL-RG"

        :return:
        """
        self.validate_sp_login()
        print("Creating DB...")
        self.database_name = database_name

        # You MIGHT need to add SQL as a valid provider for these credentials
        # If so, this operation has to be done only once for each credential
        self.resource_client.providers.register('Microsoft.Sql')

        if options.get('resource_group_name'):
            self.default_RG = options.get('resource_group_name')

        resource_group_params = options.get('resource_group_params') or {'location': self.region}
        self.resource_client.resource_groups.create_or_update(
            self.default_RG, resource_group_params
        )

        if options.get('create_new_server'):
            self.create_sql_server_instance(**options)
        if options.get('set_firewall_rules'):
            self.create_firewall_rule(**options)

        try:
            async_db_create = self.sql_client.databases.create_or_update(
                resource_group_name = self.default_RG,
                server_name         = self.sql_server_name,
                database_name       = self.database_name,
                parameters          =
                {
                    'location' : options.get('region') or self.region,
                    'sku'      : self.sku
                }
            )
        except Exception as err:
            print(f"Error creating database")
            raise Exception(err.args)
        finally:
            self.db_object = async_db_create.result()
            self.set_jdbc_url()
            print(self.db_object)

    def create_sql_server_instance(self, sql_server_name = None, **options):
        """
        Create SQL server instance
        :return:
        """
        print("Creating Server Instance... This might take 4-5 minutes")

        self.sql_server_name = sql_server_name if sql_server_name else self.sql_server_name
        self.username = options.get('username') or self.username or "test"
        self.password = options.get('admin_password') or self.password or "MyPassword1@"

        server_details = self.sql_client.servers.create_or_update(
            self.default_RG,
            self.sql_server_name,
            {
                'location': options.get('region') or self.region,
                'version': '12.0',
                'administrator_login': self.username,
                'administrator_login_password': self.password
            }
        )

        result = server_details.result()
        print(f"server details -> {result}")
        return server_details

    def create_firewall_rule(self,
                             starting_ip = None,
                             ending_ip   = None,
                             **options):
        """
        Firewall rule is required to enable IP addresses to be able to connect to SQL
        If starting and end IP is not given, current external IP will be used to set the rule.
        :param options:
        :param starting_ip: IP range on which the access is allowed
        :param ending_ip: Up to IP for which access is allowed
        :return:
        """
        print("Creating Server Firewall Rule...")
        try:
            assert starting_ip and ending_ip
        except AssertionError:
            import requests
            ip = requests.get('https://checkip.amazonaws.com').text.strip()
            print(f"Starting IP and ending IP is not given, using current IP of the system which is {ip}")
            subnet = ".".join(ip.split('.')[0:2])
            starting_ip = subnet + ".0.0"
            ending_ip   = subnet + ".255.255"
            print(f"Creating firewall rule with starting IP: {starting_ip} and ending IP: {ending_ip}")
        try:
            firewall_rule = self.sql_client.firewall_rules.create_or_update(
                self.default_RG,
                options.get('server_name') or self.sql_server_name,
                f"firewall_from_{starting_ip}_to_{ending_ip}",
                f"{starting_ip}",
                f"{ending_ip}"
            )
        except Exception as err:
            print(f"Error creating firewall rule, info -> {err.args[0]}")
        finally:
            print(f"Firewall rule created, info -> {firewall_rule}")
            return firewall_rule

    def define_sku(self, name = 'Free', tier = 'Free', size = None, family = None, capacity = None):
        """
        Define the sku of the DB you want to create. By default it will create free DB.
        :param name: Required. The name of the SKU, typically, a letter + Number code, e.g. P3.
        :param tier: The tier or edition of the particular SKU, e.g. Basic, Premium.
        :param size: Size of the SKU
        :param family: If the service has different generations of hardware, for the same SKU, then that can
         be captured here.
        :param capacity: Capacity of the SKU
        :return:
        """
        self.sku = Sku(name = name, tier = tier, size = size, family = family, capacity = capacity)

    def get_database(self, database_name, **options):
        """
        Get Database object from Azure
        :param database_name: Name of the database
        :param options:
        :return:
        """
        self.db_object = self.sql_client.databases.get(
            options.get('resource_group_name') or self.default_RG,
            options.get('server_name') or self.sql_server_name,
            database_name
        )

    def set_connection_object(self):
        """
        Sets the connection object for your DB
        :return:
        """
        assert self.jdbc_url, "JDBC URL is not set, cannot get a connection without it, use set_jdbc_url() method to" \
                              "set / get jdbc URL."
        self.conn   = self.check_connection(return_result=True)
        self.cursor = self.conn.cursor()

    def execute_raw_query(self, query, print_results = True):
        """
        Execute raw query on your db using the conn object
        :param query:
        :param print_results: Turn off printing of queries
        :return:
        """
        assert self.conn and self.cursor, "Use set_connection_object() method to set 'conn' and 'cursor' object"
        if print_results:
            print(f"Executing query : {query}")
        self.cursor.execute(query)

    @beartype
    def commit_data(self, data: (list, dict), **options):
        """
        Send the data to the DB that you have initialized. self._schema is necessary to be defined for this function.
        :param data: Data can be either of the type dict, or list. If data is list, all values should be provided
                     for the columns of the table, else it will raise SQL error.
                     If do not want to give values for all the columns, use dict, where an element looks like
                     [{ 'col_name' : <col-name>, 'value' : <value> }], and it will create query for only those cols
                     or a dict can be passed {'col_name_1': 'value_1', 'col_name_2': 'value_2'....}
        :return:
        """
        assert self._schema, "Please provide a schema by using create_table_schema() method"
        query = f"INSERT INTO {self.table} ("
        if isinstance(data, dict):
            for col_name in data.keys():
                query += f"{col_name}, "
            query = query[0:-2] + ") VALUES ("
            for values in data.values():
                query += f"'{values}', "
            query = query[0:-2] + ")"
            self.execute_raw_query(query)

        elif isinstance(data[0], dict):
            for col in data:
                query += f"{col['col_name']}, "
            query = query[0:-2] + ") VALUES ("
            for col in data:
                query += f"'{col['value']}', "
            query = query[0:-2] + ")"
            self.execute_raw_query(query)

        else:
            assert len(self._schema) == len(data), "All columns values are not given, as number of elements in 'data'" \
                                                   "is not equal to number of cols defined in _schema"
            for col in self._schema:
                query += f"{col['col_name']}, "
            query = query[0:-2] + ") VALUES ("
            for _data in data:
                query += f"'{_data}', "
            query = query[0:-2] + ")"
            self.execute_raw_query(query)

    def set_jdbc_url(self):
        """
        Create JDBC URL of your database
        :return:
        """
        assert self.sql_server_name and self.database_name and self.username and self.password, \
            "Please set all credentials (server name, database name, username and password) of the database " \
            "you want to connect, then only you can get JDBC url. Use provide_sql_credentials() method"

        self.jdbc_url = create_jdbc_url(
            host_name     = f"{self.sql_server_name}.database.windows.net",
            database_name = self.database_name,
            username      = f"{self.username}@{self.sql_server_name}",
            password      = self.password
        ) + "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
        print("JDBC URL: " + self.jdbc_url)

    def validate_sp_login(self):
        """
        Validate if the user has logged in using his service principal credentials and the env variables are set or not
        :return:
        """
        assert self.subscription_id and self.client_id and self.client_secret and self.tenant_id, \
            f"Please login using 'login()' method and passing your service principal credentials, as AzureSQL " \
            f"class uses SP_credentials which are exported as environment variables, and current values of those " \
            f"variables are subscription_id -> {self.subscription_id}, tenant_id -> {self.tenant_id}," \
            f"client_id -> {self.client_id}," \
            f"client_secret -> {self.client_secret}"

    @beartype
    def _validate_table_schema_structure(self, schema: list) -> bool :
        """
        Validate the structure of the schema
        :param schema:
        :return:
        """

        for col_value in schema:
            assert isinstance(col_value, dict), f"All values of schema, should be a dict, {col_value} is not"
            assert len({'col_name', 'datatype'} - set(col_value.keys())) == 0, \
                f"'col_name and datatype must be present, they are not in {col_value}"

            # lower case the name of the column and store it in new temp dict.
            self._schema.append({
                'col_name': col_value['col_name'].lower(),
                'datatype': col_value['datatype']
            })
        return True