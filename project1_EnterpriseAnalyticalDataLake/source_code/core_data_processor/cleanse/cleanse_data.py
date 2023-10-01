from pyspark.sql import functions as f

class CleanseData:
    def cleanse_remove_row_when_header_column0_and_value_equal(self, dataframe):
        df_res = dataframe.filter(f.col(dataframe.columns[0]) != '"{}"'.format(dataframe.columns[0]))
        return df_res

    def cleanse_remove_double_quotes(self, dataframe, column_name):
        df_res = dataframe.withColumn(column_name, f.regexp_replace(f.col(column_name), '"', ''))
        return df_res
