from pyspark.sql import functions as f

class TransformDateFormat:
    def transform_dateformat_yymmdd_to_yyyyMMdd(self, dataframe, list_column_name):
        df_res = dataframe
        for column_name in list_column_name:
            df_res = df_res.withColumn(column_name, f.expr("CASE WHEN  {} > '800000' THEN  19000000+{} ELSE 20000000+{} END".format(column_name,column_name,column_name))) 
            df_res = df_res.withColumn(column_name, f.col(column_name).cast('long')). \
            withColumn(column_name, f.col(column_name).cast('string')). \
            withColumn(column_name, f.to_date(f.col(column_name), "yyyyMMdd"))
        
        return df_res
    
    def transform_timestampformat_yymmdd_mmssms_to_yyyymmdd_mmssms(self, dataframe, list_column_name):
        df_res = dataframe
        for column_name in list_column_name:
            temp_column_name = "temp_{}".format(column_name)
            df_res = df_res.withColumn(temp_column_name, f.substring(column_name, 0, 6))
            df_res = df_res.withColumn(temp_column_name, f.expr(
                "CASE WHEN  {} > '800000' THEN  19000000+{} ELSE 20000000+{} END".format(temp_column_name,
                                                                                         temp_column_name,
                                                                                         temp_column_name)))
            df_res = df_res.withColumn(temp_column_name, f.col(temp_column_name).cast('long').cast('string')). \
                withColumn(temp_column_name,
                           f.concat_ws('-', f.substring(temp_column_name, 0, 4), f.substring(temp_column_name, 5, 2),
                                       f.substring(temp_column_name, 7, 2))). \
                withColumn(column_name, f.to_timestamp(
                f.concat_ws('', temp_column_name, f.substring(column_name, 7, 9), f.lit('.000')))). \
                drop(temp_column_name)
        return df_res
    