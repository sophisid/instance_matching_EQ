# ------------------ Utility Functions ------------------

import math
from rdflib import Graph, URIRef, Namespace
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from config import sparql, GEONAMES_USERNAME, EARTHQUAKE_MODEL


def haversine(lat1, lon1, lat2, lon2):
    """Calculate the great-circle distance (in km) between two points."""
    R = 6371  # Earth's radius in km
    dLat = math.radians(lat2 - lat1)
    dLon = math.radians(lon2 - lon1)
    a = math.sin(dLat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dLon/2)**2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def insert_same_as(entity1, entity2, typeEntity):
    """Insert an owl:sameAs triple linking two entities."""
    graph_name = f"https://crm-eq.ics.forth.gr/ontology#/custom/{typeEntity}"
    
    
    update_query = f"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX custom: <{EARTHQUAKE_MODEL}/custom/>
    INSERT DATA {{
        GRAPH custom:{typeEntity} {{
            <{entity1}> owl:sameAs <{entity2}> .
        }}
    }}
    """
    sparql.setQuery(update_query)
    sparql.setMethod(POST)
    sparql.query()

def insert_close_match(entity1, entity2, typeEntity):
    """Insert a custom:closeMatch triple linking two similar entities."""
    graph_name = f"https://crm-eq.ics.forth.gr/ontology#/custom/{typeEntity}"
    
    update_query = f"""
    PREFIX custom: <{EARTHQUAKE_MODEL}/custom/>
    INSERT DATA {{
        GRAPH custom:{typeEntity} {{
            <{entity1}> custom:closeMatch <{entity2}> .
        }}
    }}
    """
    sparql.setQuery(update_query)
    sparql.setMethod("POST")
    sparql.query()

