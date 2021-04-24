from azure.mgmt.sql import SqlManagementClient
from .common_utils import *
from azure.mgmt.sql.models import Sku
from azure_utilities.identity import Identity


class CreateSQLInstance:
    """
    A class to create SQL instance in Azure. Currently supports only 'Azure SQL', not 'SQL virtual Machine' and
    'Managed Instance'
    """
    @beartype
    def __init__(self,
                 sql_credentials : SQLCredentials ,
                 identity : Identity              ,
                 **options):
        self.sql_credentials = sql_credentials
        self.default_RG      = options.get('resource_group_name') or "default_rg_python"
        self.region          = options.get('region') or "westus"
        self.sku             = options.get('sku') or Sku(name='Free')
        self.db_object       = None

        self.resource_client = identity.resource_client
        self.sql_client      = SqlManagementClient(identity.credentials, identity.subscription_id)

    def create_sql_server_instance(self, **options):
        """
        Create SQL server instance
        :return:
        """
        print("Creating Server Instance... This might take 4-5 minutes")
        server_details = self.sql_client.servers.create_or_update(
            self.default_RG,
            self.sql_credentials.server_name,
            {
                'location': self.region,
                'version': '12.0',
                'administrator_login': self.sql_credentials.db_username,
                'administrator_login_password': self.sql_credentials.db_password
            }
        )

        result = server_details.result()
        print(f"server details -> {result}")
        return server_details

    def create_firewall_rule(self, starting_ip = None, ending_ip = None, **options):
        """
        Firewall rule is required to enable IP addresses to be able to connect to SQL
        If starting and end IP is not given, current external IP will be used to set the rule.
        :param starting_ip: IP range on which the access is allowed
        :param ending_ip: Up to IP for which access is allowed
        :return:
        """
        print("Creating Server Firewall Rule...")
        try:
            assert starting_ip and ending_ip
        except AssertionError:
            starting_ip = ending_ip = ""
            import requests
            ip = requests.get('https://checkip.amazonaws.com').text.strip()
            print(f"Starting IP and ending IP is not given, using current IP of the system which is {ip}")
            subnet = ".".join(ip.split('.')[0:2])
            starting_ip = subnet + ".0.0"
            ending_ip = subnet + ".255.255"
            print(f"Creating firewall rule with starting IP: {starting_ip} and ending IP: {ending_ip}")
        try:
            firewall_rule = self.sql_client.firewall_rules.create_or_update(
                self.default_RG,
                self.sql_credentials.server_name,
                f"firewall_from_{starting_ip}_to_{ending_ip}",
                f"{starting_ip}",
                f"{ending_ip}"
            )
        except Exception as err:
            print(f"Error creating firewall rule, info -> {err.args[0]}")
        finally:
            print(f"Firewall rule created, info -> {firewall_rule}")
            return firewall_rule

    def create_sql_db(self, **options):
        """
        Create a SQL DB in azure. If server name is not given, program will throw an error, else pass optional params
        of create_new_server=True and server_name=<Server-name> or call create_new_server method.
        Provide username and password either through the params, or "provide_sql_credentials" method
        and passing neccessary detail.

        optional params:
                resource_group_params: Parameters for creating or updating a resource group
                resource_group_name: Name of the resource group, default "default-rg-python"
                create_new_server : creates a new server and prints the info
                set_firewall_rules : sets the firewall rules for the newly created server

        :return:
        """
        validate_sp_login()
        print("Creating DB...")

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
                server_name         = self.sql_credentials.server_name,
                database_name       = self.sql_credentials.database_name,
                parameters          =
                {
                    'location': options.get('region') or self.region,
                    'sku': self.sku
                }
            )
        except Exception as err:
            print(f"Error creating database")
            raise Exception(err.args)
        finally:
            self.db_object = async_db_create.result()

            print(self.db_object)

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
