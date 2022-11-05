import json
from pyspark.sql import Row
import requests
from bs4 import BeautifulSoup


class Extractor:
    def __init__(self, spark):
        self.endpoint = 'http://www.imdb.com'
        self.spark = spark

    def extract(self):
        movies_top_response = self.make_request('/chart/top/')
        movies_top_parser = self.get_parser(movies_top_response)
        movies = movies_top_parser.select('td.titleColumn')
        i = 0
        movies_list = []
        for movie in movies:
            i += 1
            print(i)
            movie_info_url = movie.a['href']
            movie_info = self.make_request(movie_info_url)
            row = self.create_new_entry(movie_info)
            movies_list.append(row)
        return self.create_df_from_list(movies_list)

    def create_new_entry(self, movie_info):
        json_str = self.get_parser(movie_info).select('script')[2].get_text()
        return self.str_to_dict(json_str)

    def make_request(self, suffix):
        return requests.get(f'{self.endpoint}/{suffix}').text

    def create_df_from_list(self, data_list):
        data_as_json = self.spark.sparkContext.parallelize(data_list)
        return self.spark.read.json(data_as_json)

    @staticmethod
    def write(df, output='./resources/raw_data'):
        df.write.parquet(output)

    @staticmethod
    def get_parser(html_doc):
        return BeautifulSoup(html_doc, "html.parser")

    @staticmethod
    def str_to_dict(json_string):
        return json.loads(json_string)
