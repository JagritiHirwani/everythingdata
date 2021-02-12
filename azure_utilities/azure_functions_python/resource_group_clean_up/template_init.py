import datetime
import logging

import azure.functions as func
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from azure.mgmt.resource import ResourceManagementClient
from azure.common.credentials import ServicePrincipalCredentials
import smtplib


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    subscription_id = '$AZURE_SUBSCRIPTION_ID'
    client_id       = '$AZURE_CLIENT_ID'
    client_secret   = '$AZURE_CLIENT_SECRET'
    tenant_id       = '$AZURE_TENANT_ID'

    credentials = ServicePrincipalCredentials(
        client_id=client_id,
        secret=client_secret,
        tenant=tenant_id
    )

    resource_client = ResourceManagementClient(credentials, subscription_id)
    try:
        for rg in resource_client.resource_groups.list():
            if rg.name == "$rg_name":
                print("Following resource will be deleted")
                for item in resource_client.resources.list_by_resource_group(rg.name):
                    print("Item Name: %s, Item Type: %s, Item Location : %s, RG Name : %s,"
                          % (item.name, item.type, item.location, "$rg_name"))
                resource_client.resource_groups.delete("$rg_name")
    except Exception as err:
        print(err.args)

    logging.info('Python timer trigger function ran at %s', utc_timestamp)
    server    = smtplib.SMTP_SSL('smtp.gmail.com', 465)
    send_from = "$email_id"
    password  = "$password"

    if send_from != "None":

        subject = "Deleted all the resources for resource group $rg_name"
        body    = "Hi, this is the Azure Function, I have deleted all the resources for <br>" \
                  "<h2>resource group : $rg_name </h2><br>" \
                  "cause you forgot too. :D"

        msg            = MIMEMultipart()
        msg['From']    = send_from
        msg['To']      = send_from
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'html'))

        text = msg.as_string()

        server.login(user=send_from, password=password)
        server.sendmail(send_from, send_from, text)
        logging.info("Email sent!")
        server.close()
