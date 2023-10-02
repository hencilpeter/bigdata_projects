from pyspark.sql import functions as f

class TransformDateFormat:
    def transform_dateformat_yymmdd_to_yyyyMMdd(self, dataframe, list_column_name):
        df_res = dataframe
        for column_name in list_column_name:
            df_res = dataframe.withColumn(column_name, f.expr("CASE WHEN  {} > '800000' THEN  19000000+{} ELSE 20000000+{} END".format(column_name,column_name,column_name))) 
            df_res = df_res.withColumn(column_name, f.col(column_name).cast('long')). \
            withColumn(column_name, f.col(column_name).cast('string')). \
            withColumn(column_name, f.to_date(f.col(column_name), "yyyyMMdd"))
        
        return df_res
    
    