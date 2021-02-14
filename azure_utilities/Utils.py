import names
import os
import time
from datetime import datetime as dt, timezone
from common.plot_live_data import PlotLiveData
from package_utils import ROOT_DIR
from azure_utilities import logger
from azure_utilities.azure_sql.sql_send_data import SQLSendData
from azure_utilities.azure_sql.sql_get_data import SQLGetData


def az_cli(args_str, return_result=True):
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


def clean_up_all_resources_by_azure_functions(email_id          = "None",
                                              password_of_email = "None",
                                              rg_name           = "sql",
                                              run_at_cron_exp   = "0 30 19 * * *",
                                              name_of_trigger   = "mytimer",
                                              **options):
    """
    If you are forgetful like me, use this function to deploy an azure function which will delete all the azure
    resources of the specified resource group. This will run daily at a certain time, default = 7:30 PM Daily UTC +00:00
    and will delete all the resources under a resource group if you forget to do so at the end of day.
    :param rg_name: Name of the resource group to clean up
    :param password_of_email:
    :param name_of_trigger: Name of the trigger. [optional]
    :param run_at_cron_exp: 6 figures cron tab expression for the function to run [optional]
    :param email_id: If you want to receive an email, it will use your credentials to send an email to itself.
    It uses MIMEMultipart and SMTP connection, hence requires both email and password.
    :return:
    """
    import random
    init_function_location = f"{ROOT_DIR}/azure_utilities/azure_functions_python" \
                             f"/resource_group_clean_up/template_init.py"
    function_json_location = f"{ROOT_DIR}/azure_utilities/azure_functions_python/resource_group_clean_up/function.json"

    file_content = open(init_function_location, "r").read()

    modified_file_content = file_content\
        .replace("$email_id", email_id)\
        .replace("$password", password_of_email)\
        .replace("$rg_name", rg_name)\
        .replace("$AZURE_SUBSCRIPTION_ID", os.environ.get('AZURE_SUBSCRIPTION_ID')) \
        .replace("$AZURE_CLIENT_ID", os.environ.get('AZURE_CLIENT_ID')) \
        .replace("$AZURE_CLIENT_SECRET", os.environ.get('AZURE_CLIENT_SECRET')) \
        .replace("$AZURE_TENANT_ID", os.environ.get('AZURE_TENANT_ID'))
    with open(init_function_location.replace("template_init", "__init__"), "w") as init_file:
        init_file.write(modified_file_content)

    function_file_content = open(function_json_location, "r").read()
    modified_function_file_content = function_file_content\
        .replace("$name", name_of_trigger).replace("$schedule", run_at_cron_exp)
    with open(function_json_location, "w") as json_file:
        json_file.write(modified_function_file_content)

    resource_group_name  = options.get('resource_group_name') or "clean-up-resources-rg"
    region               = options.get('region') or "westeurope"
    storage_account_name = options.get('storage_account_name') or f"cleanupapp{random.randint(100, 999)}"
    function_app_name    = options.get('function_app_name') or f"cleanupapp{random.randint(100, 999)}"

    # # Create a resource group
    az_cli(f"group create --name {resource_group_name} --location {region}")

    # # Create an Azure storage account in the resource group.
    az_cli(f"storage account create --name {storage_account_name} --location "
           f"{region} --resource-group {resource_group_name} --sku Standard_LRS")

    # # Create a serverless function app in the resource group.
    az_cli(f"functionapp create --name {function_app_name} --storage-account {storage_account_name}"
           f" --consumption-plan-location {region} --resource-group {resource_group_name} --functions-version 3"
           f" --runtime python --runtime-version 3.8"
           f" --os-type linux")

    os.system(f"cd {ROOT_DIR}/azure_utilities/azure_functions_python && "
              f"func azure functionapp publish {function_app_name} --force ")
    print(f"If the deployment fails, please execute this in your terminal -> "
          f"cd {ROOT_DIR}/azure_utilities/azure_functions_python && "
          f"func azure functionapp publish {function_app_name} --force ")


def clean_up_resources(resource_group_name):
    """
    Delete the resources associated with the resource group
    :param resource_group_name:
    :return:
    """
    from azure.common.credentials import ServicePrincipalCredentials
    from azure.mgmt.resource import ResourceManagementClient
    subscription_id = os.environ.get('AZURE_SUBSCRIPTION_ID')
    client_id       = os.environ.get('AZURE_CLIENT_ID')
    client_secret   = os.environ.get('AZURE_CLIENT_SECRET')
    tenant_id       = os.environ.get('AZURE_TENANT_ID')
    credentials = ServicePrincipalCredentials(
        client_id = client_id,
        secret    = client_secret,
        tenant    = tenant_id
    )
    resource_client = ResourceManagementClient(credentials, subscription_id)
    try:
        for rg in resource_client.resource_groups.list():
            if rg.name == resource_group_name:
                print("Following resource will be deleted")
                for item in resource_client.resources.list_by_resource_group(rg.name):
                    print("Item Name: %s, Item Type: %s, Item Location : %s, RG Name : %s,"
                          % (item.name, item.type, item.location, resource_group_name))
                resource_client.resource_groups.delete(resource_group_name)
    except Exception as err:
        print(err.args)


# # SQL class Example
if __name__ == "__main__":
    app = {
       'appId'       : 'a9b31f0c-3c8c-440e-bffa-1852d62535d3',
       'displayName' : 'Hannah-App',
       'name'        : 'http://Hannah-App',
       'password'    : 'lVKjqCd._lwh93uA.STxv8zS70u2xsE4dG',
       'tenant'      : 'bb7d3766-d430-4c13-8dc7-e8f0d774c1bb'
    }
    login(service_principal_login=True, SP_credentials=app)

    az_sql = SQLSendData(
        database_name = "sajaldb",
        server_name   = "sajal-server",
        db_username   = "test",
        db_password   = "Igobacca1@",
        resource_group_name = 'sql',
        table_name = "customer"
    )

    # create a new sql database
    # az_sql.create_sql_db(
    #     create_new_server=True,
    #     set_firewall_rules=True
    # )

    # check if the application is able to reach the DB
    az_sql.check_connection()
    # #
    # create a table schema
    az_sql.create_table_schema(schema_list = [
        {
            'col_name': 'CUST_ID',
            'datatype': 'INTEGER'
        },
        {
            'col_name': 'NAME',
            'datatype': 'VARCHAR(50)'
        }
    ])

    # create a table using the schema defined
    # az_sql.create_table_using_schema(table_name="customer")

    # az_sql.connect_to_table("customer")
    az_sql.commit_data(data = {
        'cust_id': 50,
        'name': 'sajal'
    })

    # get data from the table
    get_data = SQLGetData(
        database_name="sajaldb",
        server_name="sajal-server",
        db_username="test",
        db_password="Igobacca1@",
    )
    get_data.connect_to_table("customer")

    # # Plot live data
    # plt = PlotLiveData(get_data.return_differential_data, az_sql)
    # plt.plot_data("cust_id")
    get_data.set_alert_on_live_data(parameter_name = "cust_id", threshold = 5, alert_type = ['email'],
                                    email_sender_credential =
                                    {
                                        'email_id': 'python.package.alert@gmail.com',
                                        'password': 'Mystrongpassword1@'
                                    },
                                    send_to = 'sirohisajal@gmail.com')

    clean_up_all_resources_by_azure_functions(email_id = "python.package.alert@gmail.com",
                                              password_of_email="Mystrongpassword1@")
