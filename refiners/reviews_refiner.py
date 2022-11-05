from refiners.base_refiner import BaseRefiner
from pyspark.sql.functions import col


class ReviewsRefiner(BaseRefiner):
    def refine(self, dataframe):
        return dataframe.select(
            col("name").alias("movie_name"),
            col("review.dateCreated").cast("date").alias("creation_date"),
            col("review.inLanguage").alias("language"),
            col("review.name").alias("summary"),
            col("review.reviewBody").alias("body")
        )
