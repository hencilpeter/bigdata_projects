
class UtilHiveTable:
    def refresh_table(self, spark_session, table_name):
        spark_session.sql("msck repair table {}".format(table_name))

    def add_partition(self, spark_session, table_name, partition):
        sql_script = "ALTER TABLE {} ADD IF NOT EXISTS PARTITION ({})".format(table_name, partition)
        spark_session.sql(sql_script)
        
        
    def get_table_column_names(self, spark_session, table_name):
       
        result = spark_session.sql("DESCRIBE {}".format(table_name))
     
        column_names = [row['col_name'] for row in result.collect()]
        last_index = column_names.index('# Partition Information')
        return column_names[0:last_index]
