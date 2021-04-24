from common.plot_live_data import PlotLiveData
from azure.iot.device import IoTHubDeviceClient


if __name__ == "__main__":
    from azure_utilities.azure_nosql.cosmosdb.send_cosmos_data import SendCosmosData
    from azure_utilities.azure_nosql.cosmosdb.get_cosmos_data import GetCosmosData
    import time

    ss = SendCosmosData(
        uri="https://sajalsirohi.documents.azure.com:443/",
        key="NlEvJ5rDRDwBqwZv6Q3Sr8kenY5EYugp6QkJWiKv7M3Iy6TT9jMOiqso5tBzMqRUiR3vaabLGagPuNvC21KuIA==",
        partition_key_path="/name"
    )
    # ss.commit_batch_data(data = [
    #     {'name': 'sirohi', 'last_name': 'hehe part 2'},
    #     {'name': 'sajal', 'last_name': 'john smith'},
    #     ]
    # )
    gg = GetCosmosData(
        uri="https://sajalsirohi.documents.azure.com:443/",
        key="NlEvJ5rDRDwBqwZv6Q3Sr8kenY5EYugp6QkJWiKv7M3Iy6TT9jMOiqso5tBzMqRUiR3vaabLGagPuNvC21KuIA==",
    )
    # data = gg.get_all_data()

    # gg.set_alert_on_live_data(parameter_name="itr", threshold=2,
    #                           email_sender_credential=
    #                           {
    #                               'email_id': 'python.package.alert@gmail.com',
    #                               'password': 'Mystrongpassword1@'
    #                           },
    #                           send_to='sirohisajal@gmail.com',
    #                           ss=ss,
    #                           lesser_than = 3
    #                           )

    # # Plot your data
    plt = PlotLiveData(gg.return_differential_data)
    # plt.plot_data("itr", ss=ss)
    plt.dash_plot(["itr", "itr_", "val"], ss=ss)
