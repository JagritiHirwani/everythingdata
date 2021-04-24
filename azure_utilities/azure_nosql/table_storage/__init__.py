from azure_utilities.identity import Identity

if __name__ == "__main__":
    from azure_utilities.azure_nosql.table_storage.send_table_data import SendTableData
    from azure_utilities.azure_nosql.table_storage.get_table_data import GetTableData

    # # Send data to table storage
    # app = {
    #     'appId': 'a9b31f0c-3c8c-440e-bffa-1852d62535d3',
    #     'displayName': 'Hannah-App',
    #     'name': 'http://Hannah-App',
    #     'password': 'lVKjqCd._lwh93uA.STxv8zS70u2xsE4dG',
    #     'tenant': 'bb7d3766-d430-4c13-8dc7-e8f0d774c1bb'
    # }
    # identity = Identity()
    # identity.login(service_principal_login=True, SP_credentials=app)
    #
    # ss = SendTableData(
    #     connection_string="DefaultEndpointsProtocol=https;AccountName=sajalsirohi;AccountKey=XzfX8bC04B7CMwrBFLNP"
    #                       "/ub3uv1Pwaag8l3CNmh++GnHf4tGU9yy7EiSxbSkw73g3HzJluAyKKvorMpwJlHFlw==;EndpointSuffix=core"
    #                       ".windows.net",
    #     identity=identity
    # )

    ss = SendTableData(
        connection_string="DefaultEndpointsProtocol=https;AccountName=sajalsirohi;AccountKey=XzfX8bC04B7CMwrBFLNP"
                          "/ub3uv1Pwaag8l3CNmh++GnHf4tGU9yy7EiSxbSkw73g3HzJluAyKKvorMpwJlHFlw==;EndpointSuffix=core"
                          ".windows.net",
    )
    #
    # ss.create_table()
    ss.ROW_KEY_GEN = True
    ss.PARTITION_KEY_GEN = True
    #
    # ss.commit_data({'name': 'sajal', 'company': 'optum'})
    # ss.commit_batch_data([
    #     {
    #         'name': 'Jagriti', 'company': 'micro', 'hostel': 'ff21'
    #     },
    #     {
    #         'sajal': 'Yes this is sajal'
    #     }
    # ])

    # # Get the data from the table

    gg = GetTableData(
        connection_string="DefaultEndpointsProtocol=https;AccountName=sajalsirohi;AccountKey=XzfX8bC04B7CMwrBFLNP"
                          "/ub3uv1Pwaag8l3CNmh++GnHf4tGU9yy7EiSxbSkw73g3HzJluAyKKvorMpwJlHFlw==;EndpointSuffix=core"
                          ".windows.net"
    )

    gg.set_alert_on_live_data(parameter_name="PartitionKey", threshold=7,
                              email_sender_credential =
                              {
                                  'email_id': 'python.package.alert@gmail.com',
                                  'password': 'Mystrongpassword1@'
                              },
                              send_to = 'sirohisajal@gmail.com',
                              ss = ss
                              )

    # import time
    # i = 0
    # while True:
    #     i += 1
    #     ss.commit_data({'name': f'sajal_{i}', 'company': f'optum_{i}'})
    #     df = gg.return_differential_data()
    #     print(df)
    #     time.sleep(3)