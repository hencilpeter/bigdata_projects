from pyspark.sql import functions as f


class TransformDatatype:
    def transform_string_to_long(self, dataframe, column_name):
        df_res = dataframe.withColumn(column_name, f.col(column_name).cast('long'))
        return df_res
