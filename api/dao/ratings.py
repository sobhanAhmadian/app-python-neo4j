from api.data import ratings
from api.exceptions.notfound import NotFoundException

from api.data import goodfellas


class RatingDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """

    def __init__(self, driver):
        self.driver = driver

    """
    Add a relationship between a User and Movie with a `rating` property.
    The `rating` parameter should be converted to a Neo4j Integer.
    """

    # tag::add[]
    def add(self, user_id, movie_id, rating):

        def create_rating(tx, user_id, movie_id, rating):
            result = tx.run(
                """
                MATCH (u:User {userId: $user_id})
                MATCH (m:Movie {tmdbId: $movie_id})
                MERGE (u)-[r:RATED]->(m)
                SET r.rating = $rating,
                    r.timestamp = timestamp()
                RETURN m { .*, rating: r.rating } AS movie
                """,
                user_id=user_id,
                movie_id=movie_id,
                rating=rating,
            ).single()

            return result

        with self.driver.session() as session:
            result = session.write_transaction(create_rating, user_id, movie_id, rating)

            if result is None:
                raise NotFoundException()

            movie = result["movie"]

            return movie

    # end::add[]

    """
    Return a paginated list of reviews for a Movie.

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.
    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.
    """

    # tag::forMovie[]
    def for_movie(self, id, sort="timestamp", order="ASC", limit=6, skip=0):

        def get_rating(tx, movie_id, sort, order, limit, skip):
            result = tx.run(
                """
                MATCH (u:User)-[r:RATED]->(m:Movie {{tmdbId: $movie_id}})
                RETURN r {{
                    .rating,
                    .timestamp,
                    user: u {{
                        .userId,
                        .name
                    }}
                }} AS review
                ORDER BY r.`{0}` {1}
                SKIP $skip 
                LIMIT $limit
                """.format(
                    sort, order
                ),
                movie_id=movie_id,
                limit=limit,
                skip=skip,
            )
            return [record["review"] for record in result]

        with self.driver.session() as session:
            return session.read_transaction(get_rating, id, sort, order, limit, skip)

    # end::forMovie[]
