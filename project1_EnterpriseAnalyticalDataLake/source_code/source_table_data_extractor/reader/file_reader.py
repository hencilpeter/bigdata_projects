class FileReader:
    @staticmethod
    def read_file(filename):
        with open(filename, "r") as my_file:
            data = my_file.read()

        return data
