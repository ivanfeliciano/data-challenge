
class Writer:
    @staticmethod
    def write(df, output, file_format='parquet'):
        return df.write.format(file_format).save(output)
