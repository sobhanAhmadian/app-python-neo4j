from api.data import popular, goodfellas
from api.exceptions.notfound import NotFoundException


class FavoriteDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """

    def __init__(self, driver):
        self.driver = driver

    """
    This method should retrieve a list of movies that have an incoming :HAS_FAVORITE
    relationship from a User node with the supplied `userId`.

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.

    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.
    """

    # tag::all[]
    def all(self, user_id, sort="title", order="ASC", limit=6, skip=0):
        def get_all_favorites(tx, user_id, sort, order, limit, skip):
            result = tx.run(
                """
                MATCH (u:User {{userId: $user_id}})-[r:HAS_FAVORITE]->(m:Movie)
                RETURN m {{
                    .*,
                    favorite: true
                }} AS movie
                ORDER BY m.`{0}` {1}
                SKIP $skip
                LIMIT $limit
                """.format(
                    sort, order
                ),
                user_id=user_id,
                skip=skip,
                limit=limit,
            )

            return [record["movie"] for record in result]

        with self.driver.session() as session:
            return session.read_transaction(
                get_all_favorites, user_id, sort, order, limit, skip
            )

    # end::all[]

    """
    This method should create a `:HAS_FAVORITE` relationship between
    the User and Movie ID nodes provided.
   *
    If either the user or movie cannot be found, a `NotFoundError` should be thrown.
    """

    # tag::add[]
    def add(self, user_id, movie_id):
        def add_to_favorite(tx, user_id, movie_id):
            result = tx.run(
                """
                MATCH (u:User {userId: $user_id})
                MATCH (m:Movie {tmdbId: $movie_id})
                MERGE (u)-[r:HAS_FAVORITE]->(m)
                ON CREATE SET u.createdAt = datetime()
                RETURN m { .*, favorite: true } AS movie
                """,
                user_id=user_id,
                movie_id=movie_id,
            ).single()

            if result is None:
                raise NotFoundException()

            return result["movie"]

        with self.driver.session() as session:
            return session.write_transaction(add_to_favorite, user_id, movie_id)

    # end::add[]

    """
    This method should remove the `:HAS_FAVORITE` relationship between
    the User and Movie ID nodes provided.

    If either the user, movie or the relationship between them cannot be found,
    a `NotFoundError` should be thrown.
    """

    # tag::remove[]
    def remove(self, user_id, movie_id):
        def remove_favorite(tx, user_id, movie_id):
            result = tx.run(
                """
                MATCH (u:User {userId: $user_id})-[r:HAS_FAVORITE]->(m:Movie {tmdbId: $movie_id})
                DELETE r
                RETURN m { .*, favorite: false } AS movie
                """,
                user_id=user_id,
                movie_id=movie_id,
            ).single()

            if result is None:
                raise NotFoundException()

            return result["movie"]

        with self.driver.session() as session:
            return session.write_transaction(remove_favorite, user_id, movie_id)

    # end::remove[]
