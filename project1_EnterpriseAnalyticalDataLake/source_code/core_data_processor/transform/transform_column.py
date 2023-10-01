from pyspark.sql import functions as f


class TransformColumn:
    def transform_initcap(self, dataframe, column_name):
        df_res = dataframe.withColumn(column_name, f.initcap(f.col(column_name)))
        return df_res

    def transform_rename_column(self,dataframe, existing_column_name, new_column_name):
        df_res = dataframe.withColumnRenamed(existing_column_name, new_column_name)
        return df_res

    def transform_append_column_with_default_value(self, dataframe, column_name, default_value):
        dataframe = dataframe.withColumn(column_name,
                                         f.current_timestamp() if default_value == 'CURRENT_TIMESTAMP' else f.lit(
                                             default_value))
        return dataframe
