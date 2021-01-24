import os
from base_classes.send_data import SendToSql
from common.sql_utilities import create_jdbc_url, check_connection

try:
    from azure.common.client_factory import get_client_from_cli_profile
    from azure.common.credentials import ServicePrincipalCredentials
    from azure.mgmt.resource import ResourceManagementClient
    from azure.mgmt.sql import SqlManagementClient
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
                 sql_db          = None,
                 username        = None,
                 password        = None,
                 **options):
        """

        :param sql_server:
        :param sql_db:
        :param username:
        :param password:
        :param options:
        """
        self.sql_server_name = sql_server_name or "test-server"
        self.sql_db          = sql_db
        self.username        = username
        self.password        = password
        self.subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
        self.client_id       = os.environ.get('AZURE_CLIENT_ID')
        self.client_secret   = os.environ.get('AZURE_CLIENT_SECRET')
        self.tenant_id       = os.environ.get('AZURE_TENANT_ID')
        self.default_RG      = options.get('resource_group_name') or "Test-SQL-RG"
        self.region          = options.get('region') or "westus"
        self.sql_client      = None
        self.resource_client = None
        self.db_object       = None
        self.jdbc_url        = None

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
                                sql_db           = None,
                                username         = None,
                                password         = None,
                                **options):
        """
        Either provide credentials using this method, or while initializing the class
        :param sql_server_name:
        :param sql_db:
        :param username:
        :param password:
        :return:
        """
        self.sql_server_name = sql_server_name
        self.sql_db          = sql_db
        self.username        = username
        self.password        = password

    def check_connection(self, **options):
        """
        Check connection to your database
        :return:
        """
        print("Checking connection....")
        assert self.jdbc_url or options.get('jdbc_url'), "JDBC URL is not set or not provided"
        if check_connection(jdbc_url=self.jdbc_url or options.get('jdbc_url'),
                            user_cred={'user': self.username, 'password': self.password}):
            print(f"Successfully Connected :D, serverName -> {self.sql_server_name}")

    def create_schema(self):
        pass

    def validate_schema(self):
        pass

    def create_sql_db(self, **options):
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
        assert options.get('database_name'), "Provide database name using database_name param"
        self.sql_db = options.get('database_name')

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
                self.default_RG,
                self.sql_server_name,
                options.get('database_name'),
                {
                    'location': options.get('region') or self.region
                }
            )
        except Exception as err:
            print(f"Error creating database")
            raise Exception(err.args)
        finally:
            self.db_object = async_db_create.result()
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
        ip = None
        try:
            assert starting_ip and ending_ip
        except AssertionError:
            import requests
            ip = requests.get('https://checkip.amazonaws.com').text.strip()
            print(f"Starting IP and ending IP is not given, using current IP of the system which is {ip}")
        try:
            firewall_rule = self.sql_client.firewall_rules.create_or_update(
                self.default_RG,
                options.get('server_name') or self.sql_server_name,
                f"firewall_rule_for_{ip}" if ip else f"firewall_from_{starting_ip}_to_{ending_ip}",
                f"{ip}" if ip else f"{starting_ip}",
                f"{ip}" if ip else f"{ending_ip}"
            )
        except Exception as err:
            print(f"Error creating firewall rule, info -> {err.args[0]}")
        finally:
            print(f"Firewall rule created, info -> {firewall_rule}")
            return firewall_rule

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

    def commit_data(self):
        pass

    def set_jdbc_url(self):
        """
        Create JDBC URL of your database
        :return:
        """
        self.jdbc_url = create_jdbc_url(
            host_name     = f"{self.sql_server_name}.database.windows.net",
            database_name = self.sql_db,
            username      = f"{self.username}@{self.sql_server_name}",
            password      = self.password
        ) + "hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

    def validate_sp_login(self):
        """
        Validate if the user has logged in using his service principal credentials and the env variables are set or not
        :return:
        """
        assert self.subscription_id and self.client_id and self.client_secret and self.tenant_id, \
            f"Please login using 'login' method and passing your service principal credentials, as AzureSQL " \
            f"class uses SP_credentials which are exported as environment variables, and current values of those " \
            f"variables are subscription_id -> {self.subscription_id}, tenant_id -> {self.tenant_id}," \
            f"client_id -> {self.client_id}," \
            f"client_secret -> {self.client_secret}"
