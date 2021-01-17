from base_classes.send_data import SendData

try:
    from azure.common.client_factory import get_client_from_cli_profile
    from azure.mgmt.resource import ResourceManagementClient
    from azure.mgmt.sql import SqlManagementClient
except ImportError:
    raise ImportError("""Use -> pip install azure-common
                                pip install azure-mgmt-sql
                                pip install azure-mgmt-resource to use sql with azure """)
