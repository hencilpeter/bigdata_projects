from reader_base import ReaderBase


class ReaderCSVasDataframe(ReaderBase):
    def read_data_from_table(self, spark_session, table_name, column_names="", condition=""):
        sql_query = "select {} from  {}  {}".format("*" if len(column_names) == 0 else column_names, table_name, "" if len(condition) == 0 else "where "+ condition )
        print("data read from the table {}".format(table_name))
        df_result = spark_session.sql(sql_query)
        return df_result
       
