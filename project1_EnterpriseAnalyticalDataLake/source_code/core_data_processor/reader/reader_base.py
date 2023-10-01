from abc import ABC, abstractmethod


class ReaderBase(ABC):

    @abstractmethod
    def read_data_from_table(self, table_name):
        raise Exception("not implemented...")

    @abstractmethod
    def read_data_from_table(self, table_name, as_of_date):
        raise Exception("not implemented...")
