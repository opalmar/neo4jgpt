import os
import logging
from retry import retry
from neo4j import GraphDatabase

STUDENTS_CSV_PATH = os.getenv("STUDENTS_CSV_PATH")
SCHOOLS_CSV_PATH = os.getenv("SCHOOLS_CSV_PATH")
COURSES_CSV_PATH = os.getenv("COURSES_CSV_PATH")
SECTIONS_CSV_PATH = os.getenv("SECTIONS_CSV_PATH")
REGISTRATIONS_CSV_PATH = os.getenv("REGISTRATIONS_CSV_PATH")
ENROLMENTS_CSV_PATH = os.getenv("ENROLMENTS_CSV_PATH")

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

LOGGER = logging.getLogger(__name__)


NODES = ["Student", "School", "Course", "Section", "Registration", "Enrolment"]

def _set_uniqueness_constraints(tx, node):
    query = f"""CREATE CONSTRAINT IF NOT EXISTS FOR (n:{node})
        REQUIRE n.id IS UNIQUE;"""
    _ = tx.run(query, {})


@retry(tries=100, delay=10)
def load_pasi_graph_from_csv() -> None:
    """Load structured student CSV data following
    a specific ontology into Neo4j"""

    driver = GraphDatabase.driver(
        NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD)
    )

    LOGGER.info("Setting uniqueness constraints on nodes")
    with driver.session(database="neo4j") as session:
        for node in NODES:
            session.execute_write(_set_uniqueness_constraints, node)



    LOGGER.info("Loading school nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{SCHOOLS_CSV_PATH}' AS schools
        MERGE (s:School {{id: toInteger(schools.school_code),
                            name: schools.school_name,
                            district_code: schools.school_district_code,
                            district_name: schools.school_district_name
        }})
            ON CREATE SET s.city = schools.school_city
            ON MATCH SET s.city = schools.school_city
            ON CREATE SET s.postal_code = schools.school_postal_code
            ON MATCH SET s.postal_code = schools.school_postal_code        
        """
        _ = session.run(query, {})

    

if __name__ == "__main__":
    load_pasi_graph_from_csv()