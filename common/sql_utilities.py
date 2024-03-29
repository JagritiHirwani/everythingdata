import jaydebeapi
from package_utils import ROOT_DIR


def create_jdbc_url(host_name, database_name, username = None, password = None ):
    """
    Returns JDBC URL based on the given input
    :param database_name:
    :param host_name: Server name of the DB
    :param username: Admin / User username for the DB
    :param password: Admin / User Password for the DB
    :return: String
    """

    url = f"jdbc:sqlserver://{host_name}:1433;database={database_name}"
    if username and password:
        url += f";user={username};password={password}"
    return url


def check_connection(jdbc_url, user_cred, **options):
    """
    Check the connection to your DB
    :param jdbc_url: JDBC URL for the DB
    :param user_cred: It is dict in format {'user': username, 'password': password}
    :return: Boolean
    """
    assert user_cred.get('user') and user_cred.get('password'), "Please provide a username and password"

    try:
        print("Trying to connect...")
        conn = jaydebeapi.connect(
            "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            jdbc_url,
            user_cred,
            options.get('jar_path') or get_jar_path(options.get('jar_version', 8))
        )
        curs = conn.cursor()

        if not options.get('skip_table_creation'):
            print("Trying to create a temp table...")
            curs.execute("IF OBJECT_ID('dbo.CUSTOMER_Test_Python', 'U') IS NOT NULL "
                         "DROP TABLE dbo.CUSTOMER_Test_Python; ")
            curs.execute('create table CUSTOMER_Test_Python'
                         '("CUST_ID" INTEGER not null,'
                         ' "NAME" VARCHAR(50) not null,'
                         ' primary key ("CUST_ID"))'
                         )
            print("Table created successfully")
            curs.execute("insert into CUSTOMER_Test_Python values (?, ?)", (1, 'John'))
            curs.execute("select * from CUSTOMER_Test_Python")
            curs.fetchall()
            print("Dropping table...")
            curs.execute('DROP table CUSTOMER_Test_Python')
        print("Connection is live and running..")
        return conn, curs
    except Exception as err:
        print('Error connecting to DB')
        raise Exception(err.args[0])


def get_jar_path(version=8):
    """
    Returns the path of the jar file by taking the version number according to the user
    :param version: 8 | 11 | 14, 8 is default
    :return: String, path to the jar file
    """
    assert version in [8, 11, 14], "Version should be 8 or 11 or 14"
    return f"{ROOT_DIR}/common/sql_driver_jars/mssql-jdbc-8.4.1.jre{version}.jar"


if __name__ == "__main__":
    pass
