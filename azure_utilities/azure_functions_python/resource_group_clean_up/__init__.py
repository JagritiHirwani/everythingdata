import datetime
import logging

import azure.functions as func
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from azure.mgmt.resource import ResourceManagementClient
from azure.common.credentials import ServicePrincipalCredentials
from azure.identity import ClientSecretCredential
import smtplib


def main(mytimer: func.TimerRequest) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    if mytimer.past_due:
        logging.info('The timer is past due!')

    subscription_id = 'b73f0eeb-2ae0-4f82-b6c0-b455fe73941e'
    client_id       = 'a9b31f0c-3c8c-440e-bffa-1852d62535d3'
    client_secret   = 'lVKjqCd._lwh93uA.STxv8zS70u2xsE4dG'
    tenant_id       = 'bb7d3766-d430-4c13-8dc7-e8f0d774c1bb'

    credentials = ClientSecretCredential(
        client_id=client_id,
        client_secret=client_secret,
        tenant_id=tenant_id
    )

    resource_client = ResourceManagementClient(credentials, subscription_id)
    items_name = []
    try:
        for rg in resource_client.resource_groups.list():
            logging.info(f"resource group name => {rg.name}")
            if rg.name == "sql":
                logging.info("Following resource will be deleted")
                for item in resource_client.resources.list_by_resource_group(rg.name):
                    logging.info("Item Name: %s, Item Type: %s, Item Location : %s, RG Name : %s,"
                                  % (item.name, item.type, item.location, "sql"))
                    items_name.append(item.name)
                delete_async = resource_client.resource_groups.begin_delete("sql")
                delete_async.wait()

        logging.info('Python timer trigger function ran at %s', utc_timestamp)
        server = smtplib.SMTP_SSL('smtp.gmail.com', 465)
        send_from = "python.package.alert@gmail.com"
        password = "Mystrongpassword1@"

        if send_from != "None":
            backslash = '\\'
            subject = "Deleted all the resources for resource group sql"
            body = f"Hi, this is the Azure Function, I have deleted all the resources for <br>" \
                   f"<h2>resource group : sql </h2><br>" \
                   f"The resources are the following <br>" \
                   f"<ul>{' '.join([f'<li>{item}<{backslash[0]}li>' for item in items_name])}</ul>" \
                   f"cause you forgot to. :D"

            msg = MIMEMultipart()
            msg['From'] = send_from
            msg['To'] = send_from
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))

            text = msg.as_string()

            server.login(user=send_from, password=password)
            server.sendmail(send_from, send_from, text)
            logging.info("Email sent!")
            server.close()
    except Exception as err:
        logging.error(err.args)
