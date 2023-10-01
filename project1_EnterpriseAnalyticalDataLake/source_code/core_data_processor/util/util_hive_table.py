
class UtilHiveTable:
    def refresh_table(self, spark_session, table_name):
        spark_session.sql("msck repair table {}".format(table_name))

    def add_partition(self, spark_session, table_name, partition):
        sql_script = "ALTER TABLE {} ADD IF NOT EXISTS PARTITION ({})".format(table_name, partition)
        spark_session.sql(sql_script)
