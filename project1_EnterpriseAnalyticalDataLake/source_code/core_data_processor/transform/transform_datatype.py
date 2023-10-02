from pyspark.sql import functions as f


class TransformDatatype:
    def transform_string_to_long(self, dataframe, list_column_name):
        df_res = dataframe
        for column_name in list_column_name:
            df_res = df_res.withColumn(column_name, f.col(column_name).cast('long'))
        return df_res
    
    def transform_string_to_double(self, dataframe, list_column_name):
        df_res = dataframe
        for column_name in list_column_name:
            df_res = df_res.withColumn(column_name, f.col(column_name).cast('double'))
        return df_res
