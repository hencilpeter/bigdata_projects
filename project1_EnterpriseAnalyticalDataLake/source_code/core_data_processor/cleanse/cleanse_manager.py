from cleanse.cleanse_data import CleanseData
from pyspark.sql import functions as f

class CleanseManager:
    def __init__(self):
        print("cleansing manager....")
        clease_data = CleanseData()
        
    def cleanse_dataframe(self, dataframe, table_name, optimize_config):
        #TODO : fix the initialization 
        clease_data = CleanseData()
        cleanse_config = optimize_config.filter((f.col("optimization_category") == "cleansing") & (f.col("table_name") == table_name))
        cleansed_dataframe = dataframe
        for row in cleanse_config.collect():
            if row["optimization_name"] == "cleanse_remove_row_when_header_column0_and_value_equal":
                cleansed_dataframe = clease_data.cleanse_remove_row_when_header_column0_and_value_equal(cleansed_dataframe)
            elif row["optimization_name"] == "cleanse_remove_double_quotes":
                list_parameter_value = str(str(row["parameter_value"]).replace("[","" ).replace("]","")).split(",")
                cleansed_dataframe = clease_data.cleanse_remove_double_quotes(cleansed_dataframe, list_parameter_value)
            elif row["optimization_name"] == "cleanse_remove_duplicate_rows":
                cleansed_dataframe = clease_data.cleanse_remove_duplicate_rows(cleansed_dataframe)
        
        return cleansed_dataframe