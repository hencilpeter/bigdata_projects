#from abc import ABC, abstractmethod


class ReaderBase():

    #@abstractmethod
    def read_data_from_table(self, spark_session, table_name, column_names, condition):
        raise Exception("not implemented...")