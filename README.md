## Everything Data
<b> Note </b> : Currently supports Azure <br> <br>

A multi-cloud package which allows you to connect to multiple clouds through single package and interact with data 
offerings easily. 

### Azure
Azure related documentation

##### Login to your azure account through this application
This package recommends using a service principal to login into your azure account with restricted access, uses the same service
to authenticate your application and interact with your services.

##### Create an identity object using service principal credentials

```python
from azure_utilities.identity import Identity
# All of these details are present in your azure portal, IAM section
# If you do not have a service principal created, you can create one and provide the details as mentioned
app = {
       'appId'       : '<your-app-id>',
       'displayName' : 'Hannah-App',
       'name'        : 'http://Hannah-App',
       'password'    : '<my-very-strong-password>',
       'tenant'      : '<your-tenant-id>'
    }
# This identity will be used when provisioning any service on azure through this app
identity = Identity()
identity.login(service_principal_login=True, SP_credentials=app)
```
If you do not have any idea on how to do so, or unable to get this credentials, login through your terminal by command
```bash
  az login
```
and then pass `create_new_sp_cred=True` as an optional parameter, eg. `identity.login(service_principal_login=True, create_new_sp_cred=True)` method.
<b> Note : </b> This is not recommended as this creates a service principal with all permissions granted, so use this only 
in `dev` and not for production.

This method sets your environmental variables 
```python
# File name = azure_utilities/identity.py

os.environ['AZURE_CLIENT_ID']       = SP_credentials['appId']
os.environ['AZURE_CLIENT_SECRET']   = SP_credentials['password']
os.environ['AZURE_TENANT_ID']       = SP_credentials['tenant']
os.environ['AZURE_SUBSCRIPTION_ID'] = self.subscription_id \
    = result[0]['id']
```

You are ready to roll when you successfully login through this method.

#### SQL
There are two major operations that we do, either to fetch the data or to send the data, hence we have two separate classes
to do the same.

##### Send data to an Azure SQL server
```python
from azure_utilities.azure_sql.sql_send_data import SQLSendData
# You do not need to have an existing table, Database, server, but it is neccessary to define all this 
# if you want to create one with these configurations.
az_sql = SQLSendData(
        database_name = "<db-name>",
        server_name   = "<server-name>",
        db_username   = "<username>",
        db_password   = "<password>",
        resource_group_name = '<rg-name>',
        table_name    = "customer",
        identity      = identity
    )
```
###### Create SQL server and Database if it does not exists

```python
# create a new sql database
    az_sql.create_sql_db(
        create_new_server=True,
        set_firewall_rules=True
    )
```
If you do not want to set the firewall rules, simply do not pass the any parameters.

Check connection to your SQL instance, using 
```python
# check if the application is able to reach the DB
az_sql.check_connection()
```
This method will set your `connection` and `cursor` object.

If you already have a table present, connect to it. By connecting, it will get the schema for the table
and will make sending data to it easy for you.
```python
az_sql.connect_to_table(table_name="customer")
```

If you do not have a table, you can first create schema like and then create the table
```python
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
az_sql.create_table_using_schema(table_name="customer")
```

After doing all this, we are ready to send the data to the SQL server. Examples:
```python
# Commit single row
az_sql.commit_data(data = {
        'cust_id': 50,
        'name': 'sajal'
    })

# Commit multiple row in one go, way no. 1
az_sql.commit_batch_data(data = [(60, 'sirohi'), (80, 'poplu')])

# Commit multiple row in one go, way no. 2
az_sql.commit_batch_data(data = [{
    'cust_id': 90, 'name': 'jagriti'
}])

# Commit multiple row in one go, way no. 3, sending the whole pandas df
import pandas as pd
df = pd.DataFrame({
    'cust_id': [100], 'name': ['devesh']
})
az_sql.commit_batch_data(data = df)
```

##### Get data from an Azure SQL server
```python
from azure_utilities.azure_sql.sql_get_data import SQLGetData
get_data = SQLGetData(
        database_name = "<your-db-name>",
        server_name   = "<you-server-name>",
        db_username   = "<username>",
        db_password   = "<password-for-the-username>",
    )

# Connect to table from which you want data.
get_data.connect_to_table("customer")

# Get the whole table data as a pandas Dataframe
df = get_data.return_data()

```