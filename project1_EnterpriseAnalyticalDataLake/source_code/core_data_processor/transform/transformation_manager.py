from pyspark.sql import functions as f

from transform.transform_column import TransformColumn
from transform.transform_datatype import TransformDatatype
from transform.transform_date_format import TransformDateFormat

import json

class TransformationManager():
    def __init__(self):
        self.transformation_date = TransformDateFormat()
        self.transformation_datatype = TransformDatatype()
        self.transformation_column = TransformColumn()
        
    def transform_dataframe(self, dataframe, table_name, optimize_config):       
        transformation_config = optimize_config.filter((f.col("optimization_category") == "transformation") & (f.col("table_name") == table_name))
        transformation_dataframe = dataframe
        for row in transformation_config.collect():
            if row["optimization_name"] == "transform_dateformat_yymmdd_to_yyyyMMdd":
                list_parameter_value = str(str(row["parameter_value"]).replace("[","" ).replace("]","")).split(",")
                transformation_dataframe = self.transformation_date.transform_dateformat_yymmdd_to_yyyyMMdd(transformation_dataframe, list_parameter_value)
            elif row["optimization_name"] == "transform_rename_column":
                parameter_value = str(row["parameter_value"]).replace("'", '"')
                dict_parameter_value = json.loads(parameter_value) 
                transformation_dataframe = self.transformation_column.transform_rename_column(transformation_dataframe, dict_parameter_value)
            elif row["optimization_name"] == "transform_initcap":
                list_parameter_value = str(str(row["parameter_value"]).replace("[","" ).replace("]","")).split(",")
                transformation_dataframe = self.transformation_column.transform_initcap(transformation_dataframe, list_parameter_value)
            elif row["optimization_name"] == "transform_string_to_long":
                list_parameter_value = str(row["parameter_value"]).replace("[","" ).replace("]","").split(",")
                list_parameter_value =  [str(item.strip()) for item in list_parameter_value]                   
                transformation_dataframe = self.transformation_datatype.transform_string_to_long(transformation_dataframe, list_parameter_value) 
            elif row["optimization_name"] == "transform_string_to_double":
                list_parameter_value = str(row["parameter_value"]).replace("[","" ).replace("]","").split(",")
                list_parameter_value =  [str(item.strip()) for item in list_parameter_value]                   
                transformation_dataframe = self.transformation_datatype.transform_string_to_double(transformation_dataframe, list_parameter_value) 
                
        transformation_dataframe = self.get_special_columns_datataframe(transformation_dataframe)
        return transformation_dataframe

        
    
    def get_special_columns_datataframe(self, dataframe):
        dict_column_name_default_value = {'created_by':'system','create_timestamp':'CURRENT_TIMESTAMP','updated_by':'system', 'update_timestamp':'CURRENT_TIMESTAMP'}
        df_transform = self.transformation_column.transform_append_column_with_default_value(dataframe, dict_column_name_default_value)
        return df_transform
        