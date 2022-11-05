from pyspark.sql import SparkSession
from extractor import Extractor
from transformer import Transformer
from reader import Reader
from writer import Writer
import os


if __name__ == '__main__':
    raw_data_file = './resources/raw_data'
    output_tables = './resources/output_tables'

    spark = SparkSession.builder.master("local[1]").appName('SparkByExamples.com').getOrCreate()
    if not os.path.exists(raw_data_file):
        extractor = Extractor(spark)
        data = extractor.extract()
        extractor.write(data, raw_data_file)

    raw_data = Reader(spark).read(raw_data_file)

    movies_df = Transformer().transform(raw_data, 'movies')
    actors_df = Transformer().transform(raw_data, 'actors')
    reviews_df = Transformer().transform(raw_data, 'reviews')
    writers_and_directors_df = Transformer().transform(raw_data, 'writers_and_directors')

    Writer.write(movies_df, f'{output_tables}/movies')
    Writer.write(actors_df, f'{output_tables}/actors')
    Writer.write(reviews_df, f'{output_tables}/reviews')
    Writer.write(writers_and_directors_df, f'{output_tables}/writers_and_directors')
