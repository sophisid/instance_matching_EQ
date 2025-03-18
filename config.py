from rdflib import Namespace
import os
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED
from dotenv import load_dotenv
EARTHQUAKE_MODEL = Namespace("https://crm-eq.ics.forth.gr/ontology#")
load_dotenv()

GEONAMES_USERNAME = os.getenv("GEONAMES_USERNAME", "sophisid")
SPARQL_ENDPOINT = os.getenv("SPARQL_ENDPOINT", "http://localhost:8898/sparql")
USERNAME = os.getenv("USERNAME", "dba")
PASSWORD = os.getenv("PASSWORD", "dba")

sparql = SPARQLWrapper(SPARQL_ENDPOINT)
sparql.setReturnFormat(JSON)
sparql.setCredentials(USERNAME, PASSWORD)
sparql.setRequestMethod(URLENCODED)