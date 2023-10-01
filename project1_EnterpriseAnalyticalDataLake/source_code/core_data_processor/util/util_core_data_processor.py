from pyspark.sql import SparkSession
import getpass

from reader.reader_csv_as_dataframe import ReaderCSVasDataframe
from cleanse.cleanse_data import CleanseData
from transform.transform_column import TransformColumn
from transform.transform_datatype import TransformDatatype
from transform.transform_date_format import TransformDateFormat
from writer.writer_parquet_format import WriterParquetFormat
from util.util_hive_table import UtilHiveTable

class UtilCoreDataProcessor:
    def __int__(self):
        self.username = None
        self.spark= None
    
    def init_spark(self):
        self.username = getpass.getuser()
        self.spark= SparkSession.builder.config('spark.shuffle.useOldFetchProtocol', 'true').config("spark.sql.warehouse.dir", "/user/{}/warehouse".format(self.username)).enableHiveSupport().getOrCreate()
        
    def process_data(self, as_of_data):
        #1. initialize spark session 
        self.init_spark()
        reader = ReaderCSVasDataframe()
        cleanser = CleanseData()
        transformation_date = TransformDateFormat()
        transformation_datatype = TransformDatatype()
        transformation_column = TransformColumn()
        writer_parquet = WriterParquetFormat()
        util_hive_table = UtilHiveTable()
        
        
        # read the configuration 
        # TODO - remove hardcoding 
        columns_staging_to_core = "filename, source_hdfs_path, source_table, target_hdfs_path, target_table, data_load_type, should_add_partition"
        condition_staging_to_core = "is_active=1"
        
        df_configuration= reader.read_data_from_table(spark_session=self.spark, table_name="hp_config.staging_to_core",  column_names=columns_staging_to_core, condition= condition_staging_to_core)
        for row in df_configuration.collect():
            #print(row["source_table"], row["target_hdfs_path"], row["source_table"], row["target_table"])
            if row["filename"] == "account":
                # read staged data 
                df_stage_data= reader.read_data_from_table(spark_session=self.spark, table_name=row["source_table"],  column_names="*", condition="as_of_date="+as_of_data)
                # print(df_stage_data.show())
                
                # apply cleansing 
                df_header_cleansed = cleanser.cleanse_remove_row_when_header_column0_and_value_equal(df_stage_data)
                df_double_quote_cleansed = cleanser.cleanse_remove_double_quotes(df_header_cleansed, "frequency")
                
                # apply transformation 
                df_transform = transformation_date.transform_dateformat_yymmdd_to_yyyyMMdd(df_double_quote_cleansed, "date")
                df_transform = transformation_column.transform_rename_column(df_transform, "date", "account_creation_date")
                df_transform = transformation_column.transform_initcap(df_transform, "frequency")
                df_transform = transformation_column.transform_append_column_with_default_value(df_transform, 'created_by', 'system')
                df_transform = transformation_column.transform_append_column_with_default_value(df_transform, 'create_timestamp', 'CURRENT_TIMESTAMP')
                df_transform = transformation_column.transform_append_column_with_default_value(df_transform, 'updated_by', 'system')
                df_transform = transformation_column.transform_append_column_with_default_value(df_transform, 'update_timestamp', 'CURRENT_TIMESTAMP')
                
                df_transform = transformation_datatype.transform_string_to_long(df_transform, 'account_id')
                df_transform = transformation_datatype.transform_string_to_long(df_transform, 'district_id')
                df_transform = transformation_datatype.transform_string_to_long(df_transform, 'as_of_date')           
                print(df_transform.show(truncate=False))
                print(df_transform.printSchema())
                       
                #write the final clean data 
                column_names = ["as_of_date", "created_by","create_timestamp","updated_by","update_timestamp","account_id","district_id","account_creation_date","frequency"]
                partition_column = "as_of_date"
                hdfs_path = "/user/itv007175/datalake/core/account/"
                writer_parquet.write_dataframe(df_transform, column_names, partition_column, hdfs_path)
                
                # add new partition 
                util_hive_table.add_partition(self.spark, row["target_table"],"as_of_date='{}'".format(as_of_data))

            #stop the session 
            self.spark.stop()
        

