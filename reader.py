
class Reader:
    def __init__(self, spark):
        self.spark = spark

    def read(self, input_path, file_format='parquet'):
        return self.spark.read.format(file_format).load(input_path)
