class ValidationParameters:

    def __init__(self):
        self.filename = None
        self.hash_algorithm = None
        self.hash_value = None
        self.column_count = None
        self.record_count = None
        self.filedata = None

    def set_filename(self, filename):
        self.filename = filename

    def set_hash_algorithm(self, hash_algorithm):
        self.hash_algorithm = hash_algorithm

    def set_hash_value(self, hash_value):
        self.hash_value = hash_value

    def set_column_count(self, column_count):
        self.column_count = int(column_count)

    def set_record_count(self, record_count):
        self.record_count = int(record_count)

    def set_filedata(self, filedata):
        self.filedata = filedata

    def get_filename(self):
        return self.filename

    def get_hash_algorithm(self):
        return self.hash_algorithm

    def get_hash_value(self):
        return self.hash_value

    def get_column_count(self):
        return self.column_count

    def get_record_count(self):
        return self.record_count

    def get_filedata(self):
        return self.filedata
