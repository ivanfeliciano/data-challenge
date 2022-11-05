from refiners.base_refiner import BaseRefiner
from pyspark.sql.functions import col


class MoviesRefiner(BaseRefiner):
    def refine(self, dataframe):
        return dataframe.select(
            col("name"),
            col("description"),
            col("aggregateRating.ratingCount").cast("long").alias("votes"),
            col("aggregateRating.ratingValue").cast("double").alias("rating"),
            col("genre")[0].alias("genre"),
            col("datePublished").cast("date").alias("date_published"),
            col("review")
        )
