from api.data import genres
from api.exceptions.notfound import NotFoundException


class GenreDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """

    def __init__(self, driver):
        self.driver = driver

    """
    This method should return a list of genres from the database with a
    `name` property, `movies` which is the count of the incoming `IN_GENRE`
    relationships and a `poster` property to be used as a background.

    [
        {
        name: 'Action',
        movies: 1545,
        poster: 'https://image.tmdb.org/t/p/w440_and_h660_face/qJ2tW6WMUDux911r6m7haRef0WH.jpg'
        }, ...
    ]
    """

    # tag::all[]
    def all(self):
        def get_genres(tx):
            result = tx.run(
                """
                MATCH (g:Genre)
                WHERE g.name <> '(no genres listed)'

                CALL {
                    WITH g
                    MATCH (g)<-[:IN_GENRE]-(m:Movie)
                    WHERE m.imdbRating IS NOT NULL AND m.poster IS NOT NULL
                    RETURN m.poster AS poster
                    ORDER BY m.imdbRating DESC LIMIT 1
                }

                RETURN g {
                    .*,
                    movies: count { (g)<-[:IN_GENRE]-(:Movie) },
                    poster: poster
                }
                ORDER BY g.name ASC
                """
            )
            return [record.value("g") for record in result]

        with self.driver.session() as session:
            genres = session.execute_read(get_genres)

            return genres

    # end::all[]

    """
    This method should find a Genre node by its name and return a set of properties
    along with a `poster` image and `movies` count.

    If the genre is not found, a NotFoundError should be thrown.
    """

    # tag::find[]
    def find(self, name):
        # TODO: Open a new session
        # TODO: Define a unit of work to find the genre by it's name
        # TODO: Execute within a Read Transaction

        return [g for g in genres if g["name"] == name][0]

    # end::find[]
