from refiners.base_refiner import BaseRefiner
from pyspark.sql.functions import col, explode


class ActorsRefiner(BaseRefiner):
    def refine(self, dataframe):
        return dataframe.select(
            col("name").alias("movie_name"),
            explode(col("actor")).alias("unparsed_actor")
        ).withColumn("actor_name", col("unparsed_actor.name")).drop("unparsed_actor")
