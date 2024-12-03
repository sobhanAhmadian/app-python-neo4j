from api.data import popular, goodfellas

from api.exceptions.notfound import NotFoundException
from api.data import popular


class MovieDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """

    def __init__(self, driver):
        self.driver = driver

    """
    This method should return a paginated list of movies ordered by the `sort`
    parameter and limited to the number passed as `limit`.  The `skip` variable should be
    used to skip a certain number of rows.

    If a user_id value is suppled, a `favorite` boolean property should be returned to
    signify whether the user has added the movie to their "My Favorites" list.
    """

    # tag::all[]
    def all(self, sort, order, limit=6, skip=0, user_id=None):
        def get_movies(tx, sort, order, limit, skip, user_id):
            favorites = self.get_user_favorites(tx, user_id)

            result = tx.run(
                """
                MATCH (m:Movie)
                WHERE m.`{0}` IS NOT NULL
                RETURN m {{ 
                    .*,
                    favorite: m.tmdbId IN $favorites
                }} AS movie
                ORDER BY m.`{0}` {1}
                SKIP $skip
                LIMIT $limit
                """.format(
                    sort, order
                ),
                skip=skip,
                limit=limit,
                user_id=user_id,
                favorites=favorites,
            )
            return [record.value("movie") for record in result]

        with self.driver.session() as session:
            return session.execute_read(get_movies, sort, order, limit, skip, user_id)

    # end::all[]

    """
    This method should return a paginated list of movies that have a relationship to the
    supplied Genre.

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.
    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.

    If a user_id value is suppled, a `favorite` boolean property should be returned to
    signify whether the user has added the movie to their "My Favorites" list.
    """

    # tag::getByGenre[]
    def get_by_genre(
        self, name, sort="title", order="ASC", limit=6, skip=0, user_id=None
    ):
        def get_movies(tx, sort, order, limit, skip, user_id, genre_name):
            favorites = self.get_user_favorites(tx, user_id)

            result = tx.run(
                """
                MATCH (m:Movie)-[:IN_GENRE]->(:Genre {{name: $name}})
                WHERE m.`{0}` IS NOT NULL
                RETURN m {{ 
                    .*,
                    favorite: m.tmdbId IN $favorites
                }} AS movie
                ORDER BY m.`{0}` {1}
                SKIP $skip
                LIMIT $limit
                """.format(
                    sort, order
                ),
                skip=skip,
                limit=limit,
                user_id=user_id,
                favorites=favorites,
                name=genre_name,
            )
            return [record.value("movie") for record in result]

        with self.driver.session() as session:
            return session.execute_read(
                get_movies, sort, order, limit, skip, user_id, genre_name=name
            )

    # end::getByGenre[]

    """
    This method should return a paginated list of movies that have an ACTED_IN relationship
    to a Person with the id supplied

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.
    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.

    If a user_id value is suppled, a `favorite` boolean property should be returned to
    signify whether the user has added the movie to their "My Favorites" list.
    """

    # tag::getForActor[]
    def get_for_actor(
        self, id, sort="title", order="ASC", limit=6, skip=0, user_id=None
    ):

        def get_movies(tx, sort, order, limit, skip, user_id, actor_id):
            favorites = self.get_user_favorites(tx, user_id)

            result = tx.run(
                """
                MATCH (p:Person {{tmdbId: $actor_id}})-[:ACTED_IN]->(m:Movie)
                WHERE m.`{0}` IS NOT NULL
                RETURN m {{ 
                    .*,
                    favorite: m.tmdbId IN $favorites
                }} AS movie
                ORDER BY m.`{0}` {1}
                SKIP $skip
                LIMIT $limit
                """.format(
                    sort, order
                ),
                skip=skip,
                limit=limit,
                user_id=user_id,
                favorites=favorites,
                actor_id=actor_id,
            )
            return [record.value("movie") for record in result]

        with self.driver.session() as session:
            return session.execute_read(
                get_movies, sort, order, limit, skip, user_id, actor_id=id
            )

    # end::getForActor[]

    """
    This method should return a paginated list of movies that have an DIRECTED relationship
    to a Person with the id supplied

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.
    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.

    If a user_id value is suppled, a `favorite` boolean property should be returned to
    signify whether the user has added the movie to their "My Favorites" list.
    """

    # tag::getForDirector[]
    def get_for_director(
        self, id, sort="title", order="ASC", limit=6, skip=0, user_id=None
    ):

        def get_movies(tx, sort, order, limit, skip, user_id, director_id):
            favorites = self.get_user_favorites(tx, user_id)

            result = tx.run(
                """
                MATCH (p:Person {{tmdbId: $director_id}})-[:DIRECTED]->(m:Movie)
                WHERE m.`{0}` IS NOT NULL
                RETURN m {{ 
                    .*,
                    favorite: m.tmdbId IN $favorites
                }} AS movie
                ORDER BY m.`{0}` {1}
                SKIP $skip
                LIMIT $limit
                """.format(
                    sort, order
                ),
                skip=skip,
                limit=limit,
                user_id=user_id,
                favorites=favorites,
                director_id=director_id,
            )
            return [record.value("movie") for record in result]

        with self.driver.session() as session:
            return session.execute_read(
                get_movies, sort, order, limit, skip, user_id, director_id=id
            )

    # end::getForDirector[]

    """
    This method find a Movie node with the ID passed as the `id` parameter.
    Along with the returned payload, a list of actors, directors, and genres should
    be included.
    The number of incoming RATED relationships should also be returned as `ratingCount`

    If a user_id value is suppled, a `favorite` boolean property should be returned to
    signify whether the user has added the movie to their "My Favorites" list.
    """

    # tag::findById[]
    def find_by_id(self, id, user_id=None):

        def get_movies(tx, user_id, id):
            favorites = self.get_user_favorites(tx, user_id)

            result = tx.run(
                """
                MATCH (m:Movie {tmdbId: $id})
                RETURN m {
                    .*,
                    actors: [ (a)-[r:ACTED_IN]->(m) | a { .*, role: r.role } ],
                    directors: [ (d)-[:DIRECTED]->(m) | d { .* } ],
                    genres: [ (m)-[:IN_GENRE]->(g) | g { .name }],
                    ratingCount: count{ (m)<-[:RATED]-() },
                    favorite: m.tmdbId IN $favorites
                } AS movie
                LIMIT 1
                """,
                favorites=favorites,
                id=id,
            ).single()

            if result is None:
                raise NotFoundException()

            return result.value("movie")

        with self.driver.session() as session:
            return session.execute_read(get_movies, user_id, id=id)

        return goodfellas

    # end::findById[]

    """
    This method should return a paginated list of similar movies to the Movie with the
    id supplied.  This similarity is calculated by finding movies that have many first
    degree connections in common: Actors, Directors and Genres.

    Results should be ordered by the `sort` parameter, and in the direction specified
    in the `order` parameter.
    Results should be limited to the number passed as `limit`.
    The `skip` variable should be used to skip a certain number of rows.

    If a user_id value is suppled, a `favorite` boolean property should be returned to
    signify whether the user has added the movie to their "My Favorites" list.
    """

    # tag::getSimilarMovies[]
    def get_similar_movies(self, id, limit=6, skip=0, user_id=None):
        # TODO: Get similar movies from Neo4j

        return popular[skip:limit]

    # end::getSimilarMovies[]

    """
    This function should return a list of tmdbId properties for the movies that
    the user has added to their 'My Favorites' list.
    """

    # tag::getUserFavorites[]
    def get_user_favorites(self, tx, user_id):

        if user_id is None:
            return []

        result = tx.run(
            """
            MATCH (:User {userId: $user_id})-[:HAS_FAVORITE]->(m:Movie)
            RETURN m.tmdbId AS id
            """,
            user_id=user_id,
        )
        return [record["id"] for record in result]

    # end::getUserFavorites[]
