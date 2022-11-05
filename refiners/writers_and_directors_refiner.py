from refiners.base_refiner import BaseRefiner
from pyspark.sql.functions import col, explode


class WritersAndDirectorsRefiner(BaseRefiner):
    def refine(self, dataframe):
        return dataframe\
            .withColumn("unparsed_director", explode(col("director")).alias("unparsed_director"))\
            .withColumn("unparsed_creator", explode(col("creator")).alias("unparsed_creator"))\
            .withColumn("director_name", col("unparsed_director.name"))\
            .withColumn("writer_name", col("unparsed_creator.name"))\
            .select(
                col("name").alias("movie_name"),
                col("director_name"),
                col("writer_name")
            )
