from pyspark.sql import functions as f


class TransformColumn:
    def transform_initcap(self, dataframe, list_column_name):
        df_res = dataframe
        for column_name in list_column_name:
            df_res = df_res.withColumn(column_name, f.initcap(f.col(column_name)))
        return df_res

    def transform_rename_column(self,dataframe, dict_exist_new_column):
        df_res = dataframe
        for existing_column in dict_exist_new_column:
            df_res = df_res.withColumnRenamed(existing_column, dict_exist_new_column[existing_column])
        return df_res

    def transform_append_column_with_default_value(self, dataframe, dict_column_name_default_value):
        df_res = dataframe
        for column_name in dict_column_name_default_value:
            default_value = dict_column_name_default_value[column_name]
            dataframe = dataframe.withColumn(column_name,
                                             f.current_timestamp() if default_value == 'CURRENT_TIMESTAMP' else f.lit(default_value))
        return dataframe

    
#     def transform_append_column_with_default_value(self, dataframe, column_name, default_value):
#         df_res = dataframe
#         dataframe = dataframe.withColumn(column_name,
#                                          f.current_timestamp() if default_value == 'CURRENT_TIMESTAMP' else f.lit(
#                                              default_value))
#         return dataframe
