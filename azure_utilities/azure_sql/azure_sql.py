import os
from base_classes.send_data import SendToSql

try:
    from azure.common.client_factory import get_client_from_cli_profile
    from azure.identity import DefaultAzureCredential
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
        self.sql_server_name = sql_server_name or "Test-server"
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

    def provide_sql_credentials(self,
                                sql_server_name  = None,
                                sql_db      = None,
                                username    = None,
                                password    = None,
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
        self.sql_db     = sql_db
        self.username   = username
        self.password   = password

    def check_connection(self):
        pass

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
        credentials          = DefaultAzureCredential()
        self.resource_client = ResourceManagementClient(credentials, self.subscription_id)
        self.sql_client      = SqlManagementClient(credentials, self.subscription_id)

        # You MIGHT need to add SQL as a valid provider for these credentials
        # If so, this operation has to be done only once for each credential
        self.resource_client.providers.register('Microsoft.Sql')

        resource_group_params = options.get('resource_group_params') or {'location': self.region}
        self.resource_client.resource_groups.create_or_update(
            options.get('resource_group_name') or self.default_RG, resource_group_params
        )


    def create_sql_server_instance(self, **options):
        """
        Create SQL server instance
        :return:
        """
        server_details = self.sql_client.servers.create_or_update(
            self.default_RG,
            self.sql_server_name,
            {
                'location': options.get('region') or self.region,
                'version': '12.0',
                'administrator_login': options.get('admin_username') or "test",
                'administrator_login_password': options.get('admin_password') or "MyPassword1@"
            }
        )
        print(f"server details -> {server_details}")
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
        ip = None
        try:
            assert starting_ip and ending_ip
        except AssertionError:
            import requests
            ip = requests.get('https://checkip.amazonaws.com').text.strip()
            print(f"Starting IP and ending IP is not given, using current IP of the system which is {ip}")
        try:
            firewall_rule = self.sql_client.servers.create_or_update_firewall_rule(
                options.get('resource_group_name') or self.default_RG,
                options.get('server_name') or self.sql_server_name,
                f"firewall_rule_for_{ip}" if ip else f"firewall_from_{starting_ip}_to_{ending_ip}",
                f"{ip}" if ip else f"{starting_ip}",
                f"{ip}" if ip else f"{ending_ip}"
            )
        except Exception as err:
            print(f"Error creating firewall rule, info -> {err.args}")
        finally:
            print(f"Firewall rule created, info -> {firewall_rule}")
            return firewall_rule

    def commit_data(self):
        pass

    def validate_sp_login(self):
        """
        Validate if the user has logged in using his service principal credentials and the env variables are set or not
        :return:
        """
        assert self.subscription_id and self.client_id and self.client_secret and self.tenant_id, \
            f"Please login using 'login' method and passing your service principal credentials, as AzureSQL " \
            f"class uses SP_credentials which are exported as environment variables, and current values of those " \
            f"variables " \
            f"are subscription_id -> {self.subscription_id}, tenant_id -> {self.tenant_id}, client_id -> {self.client_id}," \
            f"client_secret -> {self.client_secret}"
