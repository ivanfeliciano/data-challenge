from refiners.actors_refiner import ActorsRefiner
from refiners.movies_refiner import MoviesRefiner
from refiners.reviews_refiner import ReviewsRefiner
from refiners.writers_and_directors_refiner import WritersAndDirectorsRefiner


class Transformer:
    def transform(self, raw_data, table):
        refiner = self.get_refiner(table)
        return refiner.refine(raw_data)

    @staticmethod
    def get_refiner(table):
        case = {
            'actors': ActorsRefiner(),
            'movies': MoviesRefiner(),
            'reviews': ReviewsRefiner(),
            'writers_and_directors': WritersAndDirectorsRefiner()
        }

        return case[table]
