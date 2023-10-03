from pyspark.sql import SparkSession
import getpass

from reader.reader_csv_as_dataframe import ReaderCSVasDataframe
from cleanse.cleanse_manager import CleanseManager
from transform.transformation_manager import TransformationManager
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
        cleanse_manager = CleanseManager()
        transformation_manager = TransformationManager()
        writer_parquet = WriterParquetFormat()
        util_hive_table = UtilHiveTable()
        
        # read the configuration 
        # TODO - remove hardcoding 
        columns_config_staging_to_core = "filename, source_hdfs_path, source_table, target_hdfs_path, target_table, data_load_type, should_add_partition"
        condition = "is_active=1"
        columns_optimize_staging_to_core = "table_name,optimization_category,optimization_name,parameter_value,is_active"
        
        df_config_staging_to_core= reader.read_data_from_table(spark_session=self.spark, table_name="hp_config.staging_to_core",  column_names=columns_config_staging_to_core, condition= condition)
        df_optimize_staging_to_core = reader.read_data_from_table(spark_session=self.spark, table_name="hp_config.optimization_staging_to_core",  column_names=columns_optimize_staging_to_core, condition= condition)
        for row in df_config_staging_to_core.collect():
            #if row["filename"] == "account":
            #if row["filename"] == "client":
            #if row["filename"] == "disp":
            #if row["filename"] == "loan":
            #if row["filename"] == "orders":
            #if row["filename"] == "trans":
            #if row["filename"] == "district":
            if row["filename"] == "card":
                
                column_names = util_hive_table.get_table_column_names(self.spark, row["target_table"])
                # read staged data 
                df_stage_data= reader.read_data_from_table(spark_session=self.spark, table_name=row["source_table"],  column_names="*", condition="as_of_date="+as_of_data)
                df_stage_data.show(truncate=False)
                
                # cleansing
                df_cleaned_data = cleanse_manager.cleanse_dataframe(df_stage_data, row["target_table"], df_optimize_staging_to_core)
                df_cleaned_data.show(truncate=False)
                
                # tranformation 
                df_transformed_data = transformation_manager.transform_dataframe(df_cleaned_data, row["target_table"], df_optimize_staging_to_core)
                df_transformed_data.show(truncate=False)
                df_transformed_data.printSchema()
                
                #write the transformed data 
                partition_column = "as_of_date"
                writer_parquet.write_dataframe(df_transformed_data, column_names, partition_column, row["target_hdfs_path"])
                
                # add new partition 
                util_hive_table.add_partition(self.spark, row["target_table"],"as_of_date='{}'".format(as_of_data))
               
        #stop the session 
        self.spark.stop()
        

