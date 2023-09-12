import pyodbc

from config.config_manager import ConfigurationManager


class SQLDataReader:
    def __init__(self):
        self.dbuser = ConfigurationManager.get_sql_server_config("user")
        self.host_name = ConfigurationManager.get_sql_server_config("host_name")
        self.port = ConfigurationManager.get_sql_server_config("port")
        self.database = ConfigurationManager.get_sql_server_config("database")
        self.schema = ConfigurationManager.get_sql_server_config("schema")
        self.password = ConfigurationManager.get_sql_server_config("password")
        self.connection = self.get_connection()
        self.cursor = self.connection.cursor()

    # def get_connection(self):
    #     return create_engine(
    #         url="mssql+pymssql://{0}:{1}@{2}:{3}/{4}".format(
    #             self.dbuser,  self.password, self.host_name, self.port, self.database
    #         )
    #     )
    #
    # def get_connection_string(self):
    #     # connection_string = "mssql+pymssql://" + self.dbuser + ":" + self.password+ "@" + self.host_name  + ":" + self.port + "/" + self.database
    #     connection_string = "mssql+pymssql://" + self.dbuser + ":" + self.password + "@" + self.host_name + "/" + self.database
    #     #connection = sqlalchemy.create_engine(connection_string)
    #     #return connection

    def get_connection(self):
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={self.host_name};DATABASE={self.database};UID={self.dbuser};PWD={self.password}'
        conn = pyodbc.connect(connection_string)
        return conn

    def get_table_data(self, table_name):
        result = self.cursor.execute(f'SELECT * FROM {table_name}')

        header = [column[0] for column in self.cursor.description]
        data = [','.join(header) + "\r"]
        for row in result:
            data.append(','.join(row) + "\r")

        return data
