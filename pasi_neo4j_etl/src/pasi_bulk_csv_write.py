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

    LOGGER.info("Loading student nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{STUDENTS_CSV_PATH}' AS students with students where students.STUDENT_GENDER is not null
        MERGE (s:Student {{id: toInteger(students.STUDENT_ASN),
                            last_name: students.STUDENT_LAST_NAME,
                            first_name: students.STUDENT_FIRST_NAME,
                            dob: students.STUDENT_BIRTH_DATE,
                            gender: students.STUDENT_GENDER,
                            citizenship_status: students.STUDENT_CITIZENSHIP_STATUS
        }})
            ON CREATE SET s.language_at_home = students.STUDENT_LANGUAGE_AT_HOME
            ON MATCH SET s.language_at_home = students.STUDENT_LANGUAGE_AT_HOME
            ON CREATE SET s.phone_number = students.STUDENT_PHONE_NUMBER
            ON MATCH SET s.phone_number = students.STUDENT_PHONE_NUMBER
            ON CREATE SET s.phone_start_date = students.STUDENT_PHONE_START_DATE
            ON MATCH SET s.phone_start_date = students.STUDENT_PHONE_START_DATE
            ON CREATE SET s.phone_end_date = students.STUDENT_PHONE_END_DATE
            ON MATCH SET s.phone_end_date = students.STUDENT_PHONE_END_DATE
            ON CREATE SET s.country_of_citizenship = students.STUDENT_COUNTRY_OF_CITIZENSHIP
            ON MATCH SET s.country_of_citizenship = students.STUDENT_COUNTRY_OF_CITIZENSHIP                                                
        """
        _ = session.run(query, {})

    LOGGER.info("Loading school nodes")
    with driver.session(database="neo4j") as session:
        query = f"""
        LOAD CSV WITH HEADERS
        FROM '{SCHOOLS_CSV_PATH}' AS schools
        MERGE (s:School {{id: toInteger(schools.SCHOOL_CODE),
                            name: schools.SCHOOL_NAME,
                            district_code: schools.SCHOOL_DISTRICT_CODE,
                            district_name: schools.SCHOOL_DISTRICT_NAME,
        }})
            ON CREATE SET s.city = schools.SCHOOL_CITY
            ON MATCH SET s.city = schools.SCHOOL_CITY
            ON CREATE SET s.postal_code = schools.SCHOOL_POSTAL_CODE
            ON MATCH SET s.postal_code = schools.SCHOOL_POSTAL_CODE        
        """
        _ = session.run(query, {})

    

if __name__ == "__main__":
    load_pasi_graph_from_csv()