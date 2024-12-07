from api.data import people, pacino
from api.exceptions.notfound import NotFoundException


class PeopleDAO:
    """
    The constructor expects an instance of the Neo4j Driver, which will be
    used to interact with Neo4j.
    """

    def __init__(self, driver):
        self.driver = driver

    """
    This method should return a paginated list of People (actors or directors),
    with an optional filter on the person's name based on the `q` parameter.

    Results should be ordered by the `sort` parameter and limited to the
    number passed as `limit`.  The `skip` variable should be used to skip a
    certain number of rows.
    """

    # tag::all[]
    def all(self, q, sort="name", order="ASC", limit=6, skip=0):
        def get_people(tx, q, sort, order, limit, skip):
            cypher = "MATCH (p:Person)"
            if q:
                cypher += "WHERE p.name CONTAINS $q"
            cypher += """
                RETURN p {{ .* }} AS person
                ORDER BY p.`{0}` {1}
                SKIP $skip
                LIMIT $limit
            """.format(
                sort, order
            )
            result = tx.run(cypher, q=q, limit=limit, skip=skip)

            return [record["person"] for record in result]

        with self.driver.session() as session:
            return session.read_transaction(get_people, q, sort, order, limit, skip)

        return people[skip:limit]

    # end::all[]

    """
    Find a user by their ID.

    If no user is found, a NotFoundError should be thrown.
    """

    # tag::findById[]
    def find_by_id(self, id):
        def get_people(tx, id):
            result = tx.run(
                """
                MATCH (p:Person) 
                WHERE p.tmdbId = $id
                RETURN p {
                    .*,
                    actedCount: count { (p)-[:ACTED_IN]->() },
                    directedCount: count { (p)-[:DIRECTED]->() }
                } AS person           
                """,
                id=id,
            ).single()

            if result:
                return result["person"]
            else:
                raise NotFoundException("Person not found")

        with self.driver.session() as session:
            return session.read_transaction(get_people, id)

    # end::findById[]

    """
    Get a list of similar people to a Person, ordered by their similarity score
    in descending order.
    """

    # tag::getSimilarPeople[]
    def get_similar_people(self, id, limit=6, skip=0):
        def get_people(tx, id, limit, skip):
            result = tx.run(
                """
                MATCH (:Person {tmdbId: $id})-[:ACTED_IN|DIRECTED]->(m)<-[r:ACTED_IN|DIRECTED]-(p)
                WITH p, collect(m {.tmdbId, .title, type: type(r)}) AS inCommon
                RETURN p {
                    .*,
                    actedCount: count { (p)-[:ACTED_IN]->() },
                    directedCount: count {(p)-[:DIRECTED]->() },
                    inCommon: inCommon
                } AS person
                ORDER BY size(person.inCommon) DESC
                SKIP $skip
                LIMIT $limit
                """,
                id=id,
                limit=limit,
                skip=skip,
            )

            return [record["person"] for record in result]

        with self.driver.session() as session:
            return session.read_transaction(get_people, id, limit, skip)

    # end::getSimilarPeople[]
