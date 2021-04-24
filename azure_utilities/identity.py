import os

import names
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.resource import ResourceManagementClient

from azure_utilities import logger


def az_cli_(args_str, return_result=True):
    """
    Execute azure ali commands
    :param args_str: command to execute. For eg "login"
    :param return_result: Returns the result_dict
    :return:
    """
    try:
        from az.cli import az
    except ImportError:
        logger.error("Use pip install az.cli")
        print("Use pip install az.cli")
        raise ImportError("az.cli not present, install it first.")
    exit_code, result_dict, logs = az(args_str)

    # On 0 (SUCCESS) print result_dict, otherwise get info from `logs`
    if exit_code == 0:
        logger.debug(f"Command Executed -> az {args_str}\n Result -> {result_dict}")
        print(result_dict)
        if return_result:
            return result_dict
    else:
        logger.error(f"Command Executed and it failed -> az {args_str}\n Logs -> {logs}")
        print(logs)


class Identity:

    def __init__(self):
        self.resource_client = None
        self.credentials = None
        self.subscription_id = None

    def login(self,
              azure_username=None,
              azure_password=None,
              portal_login=True,
              service_principal_login=False,
              **options
              ):
        """
        Logs you into your default tenant / subscription in azure either through web portal, or using azure CLI
        or using service principal credentials for an app, or for system where web portal authentication is not possible
        :param portal_login: If set to true, you will be prompted for credentials in a web browser to login into azure
        :param azure_username: If logging in using azure cli, username is required
        :param azure_password: If logging in using azure cli, password is required
        :param service_principal_login: If set to true, application will login using the credentials provided as a form of
                                        dict. Dict should contain App name, service_principal_password and tenant id.
                                        SP_credentials => {
                                        'appId': 'a8cbd321-1149-49f5-9154-62ef149207b0',
                                        'displayName': 'Christian-App',
                              [REQUIRED]'name': 'http://Christian-App',
                              [REQUIRED]'password': 'gczWye_2aHRiOII.4iST20eECXi_0Xrpwu',
                              [REQUIRED]'tenant': 'bb7d3766-d430-4c13-8dc7-e8f0d774c1bb'
                                        } # Do not try to login through this, I have deleted this App :p
        :return:
        """
        logger.debug("Inside login function")
        if portal_login and not service_principal_login:
            az_cli_("login")
        elif service_principal_login:
            logger.debug(f"Logging using service principal credentials")

            # Check if SP_credentials is mentioned or not in options
            assert options.get('SP_credentials') or options.get('create_new_sp_cred'), \
                "Please provide service principal credentials, through SP_credentials " \
                "and passing a dict, which contains 'name', 'password' and 'tenant' or pass create_new_sp_cred which " \
                "will create a new SP_cred"

            # Create new SP if not given by user
            SP_credentials = self.create_service_principal(return_result=True, **options) if \
                options.get('create_new_sp_cred') else options['SP_credentials']

            # assert the structure of SP_credentials is same as expected.
            try:
                assert len({'name', 'password', 'tenant'} - set(SP_credentials.keys())) == 0, \
                    "Provide SP_credentials in like {'name':'http://app-name', 'password': 'XX', 'tenant': " \
                    "'tenant-id'} "
            except AttributeError as err:
                raise Exception(f"SP_credentials not created, or not provided, error: {err}")

            # assert name of the app is in form "http://<app-name>"
            assert SP_credentials['name'].find("http://") != -1, "Give name of app in format 'http://<app-name>'"

            result = az_cli_(f"login --service-principal -u {SP_credentials['name']} -p {SP_credentials['password']}"
                             f" --tenant {SP_credentials['tenant']}")

            # Set environment variables to for authorization and authentication
            os.environ['AZURE_CLIENT_ID'] = SP_credentials['appId']
            os.environ['AZURE_CLIENT_SECRET'] = SP_credentials['password']
            os.environ['AZURE_TENANT_ID'] = SP_credentials['tenant']
            os.environ['AZURE_SUBSCRIPTION_ID'] = self.subscription_id \
                = result[0]['id']
            self.credentials = ServicePrincipalCredentials(
                client_id=SP_credentials['appId'],
                secret=SP_credentials['password'],
                tenant=SP_credentials['tenant']
            )

            self.resource_client = ResourceManagementClient(self.credentials, result[0]['id'])
            os.environ['logged_in'] = 'True'

        else:
            assert azure_password and azure_username, "Please provide azure_password and azure_username if you " \
                                                      "are not going to login through portal and set portal_login " \
                                                      "= False "
            az_cli_(f"login -u {azure_username} -p {azure_password}")

    def create_service_principal(self, **options):
        """
        Call this function, after successfully logging in through "az login" in CLI.
        Currently, username and password logging can not be done, as it is not a safe way to login in your account and
        hence, microsoft has expired it unless you remove MFA from your azure account, and also it can not be done for
        MSA accounts, only for student and work accounts.

        So to login using credentials, you can create a service principle, and assign this service principal neccessary
        access to the resource, by going to "access policies" or "IAM" section of the resource.
        Refer for more information about service principals :
         https://docs.microsoft.com/en-us/azure/active-directory/develop/howto-create-service-principal-portal

        This function will create a service principal using azure cli commands and return and store the credentials returned
        in a json file for future reference to enable users to login using credentials of defined service principal
        only using the "login" function

        Note: To create a new service principal, ensure you are logged in with your global administrator account or the
        account you used while creating the azure account, using "az login".

        :return: Service principal object
        :optional params:
                          name_of_app       : Name of the service principal which will be registered
                          skip_assignment   : If not given, service principal will be given contributor
                                              access to the subscription, if set to true, no role will be assigned to SP.
                          return_result     : If set to true, result will be returned
        """
        logger.debug(f"Inside create_service_principal function, options -> {options}")
        print("Creating new service principal...")
        name_of_app = options.get('name_of_app') or f"{names.get_first_name()}-Python-App"
        skip_assignment = "--skip-assignment" if options.get('skip_assignment') else ""

        logger.debug(f"For service principle, name of App -> {name_of_app}")
        service_app_cred = az_cli_(f"ad sp create-for-rbac -n {name_of_app} {skip_assignment}")

        print(f"Service principal details -> {service_app_cred}")
        os.environ['AZURE_CLIENT_ID'] = service_app_cred['appId']
        os.environ['AZURE_CLIENT_SECRET'] = service_app_cred['password']
        os.environ['AZURE_TENANT_ID'] = service_app_cred['tenant']
        if options.get('return_result'):
            return service_app_cred
