import os
from SPARQLWrapper import SPARQLWrapper, JSON, POST, URLENCODED

# Virtuoso SPARQL endpoint (ensure it's the correct one for updates)
sparql_endpoint = "http://83.212.113.65:8898/sparql"

# Credentials for Virtuoso (change if necessary)
USERNAME = os.getenv("USERNAME", "dba")
PASSWORD = os.getenv("PASSWORD", "hy561")

# Initialize SPARQLWrapper
sparql = SPARQLWrapper(sparql_endpoint)
sparql.setReturnFormat(JSON)
sparql.setCredentials(USERNAME, PASSWORD)
sparql.setMethod(POST)
sparql.setRequestMethod(URLENCODED)

# SPARQL INSERT query (use full IRI instead of prefix)
insert_query = """
    PREFIX eq: <https://crm-eq.ics.forth.gr/ontology#>

    INSERT DATA {
      GRAPH <https://crm-eq.ics.forth.gr/ontology> {
        <https://crm-eq.ics.forth.gr/ontology#person1> a eq:E21_Person ;
                   eq:P131_identified_by "Sofia" ;
                   eq:P98i_was_born "1800-01-01" ;
                   eq:P100i_died_in "1875-12-31" .
      }
    }
"""

# Execute INSERT query
sparql.setQuery(insert_query)
try:
    response = sparql.query()
    print("INSERT successful!")
except Exception as e:
    print("INSERT failed:", e)

# SPARQL SELECT query to verify insertion
select_query = """
    PREFIX eq: <https://crm-eq.ics.forth.gr/ontology#>

    SELECT ?s ?p ?o WHERE { 
      GRAPH <https://crm-eq.ics.forth.gr/ontology> {
        ?s ?p ?o 
      }
    }
    LIMIT 10
"""

# Execute SELECT query
sparql.setQuery(select_query)
try:
    results = sparql.query().convert()
    print("SELECT Results:")
    for result in results["results"]["bindings"]:
        print(result)
except Exception as e:
    print("SELECT failed:", e)
