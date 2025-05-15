from rdflib import Namespace
from SPARQLWrapper import SPARQLWrapper, JSON, POST , URLENCODED
from fuzzywuzzy import fuzz
from dotenv import load_dotenv
import os
import argparse

from person_enrichment import get_wikidata_enrichment_data
from utils import insert_same_as, insert_close_match
from match_eq import match_earthquakes, normalize_dates
from config import sparql, GEONAMES_USERNAME


# Namespaces
EARTHQUAKE_MODEL = Namespace("https://crm-eq.ics.forth.gr/ontology#")
DATE_THRESHOLD = 2            # years allowable difference for persons

# ------------------ Matching Persons ------------------

def query_persons():
    query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    SELECT ?p ?label ?birth ?death WHERE {
      ?p a crm:E21_Person .
      ?p rdfs:label ?label .
      OPTIONAL { ?p <https://crm-eq.ics.forth.gr/ontology#P98i_was_born> ?birth . }
      OPTIONAL { ?p <https://crm-eq.ics.forth.gr/ontology#P100i_died_in> ?death . }
    }
    """
    sparql.setQuery(query)
    sparql.setMethod("GET")
    results = sparql.query().convert()
    persons = []
    for result in results["results"]["bindings"]:
        p = result["p"]["value"]
        label = result["label"]["value"]
        birth = result.get("birth", {}).get("value", None)
        death = result.get("death", {}).get("value", None)
        persons.append((p, label, birth, death))
    return persons

def query_persons_with_wikidata():
    """
    Query persons and retrieve any Wikidata resource linked via custom:closeMatch, ensuring distinct results.
    """
    query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    PREFIX custom: <https://crm-eq.ics.forth.gr/ontology#/custom/>
    SELECT ?p ?label ?birth ?death ?w WHERE {
      ?p a crm:E21_Person .
      ?p rdfs:label ?label .
      OPTIONAL { ?p <https://crm-eq.ics.forth.gr/ontology#P98i_was_born> ?birth . }
      OPTIONAL { ?p <https://crm-eq.ics.forth.gr/ontology#P100i_died_in> ?death . }
      OPTIONAL { ?p custom:closeMatch ?w .
                 FILTER(regex(str(?w), "http://www.wikidata.org/entity/"))
      }
    }
    """
    sparql.setQuery(query)
    sparql.setMethod("GET")
    results = sparql.query().convert()
    persons = []
    for result in results["results"]["bindings"]:
        p = result["p"]["value"]
        label = result["label"]["value"]
        birth = result.get("birth", {}).get("value", None)
        death = result.get("death", {}).get("value", None)
        wikidata_uri = result["w"]["value"] if "w" in result else None
        persons.append((p, label, birth, death, wikidata_uri))
        # print(f"Person: {p}, Label: {label}, Birth: {birth}, Death: {death}, Wikidata: {wikidata_uri}")
    return persons

def update_person_with_wikidata_data(person_uri, wikidata_data):
    """
    Create a new Wikidata resource and update the endpoint so that:
      ?localPerson owl:sameAS ?wikidataPerson .
      ?wikidataPerson has all the enrichment data (label, birth/death dates) and occupations.
    """
    if not wikidata_data:
        return
 
    graph_name = "https://crm-eq.ics.forth.gr/ontology#/custom/wikidata"

    wikidata_uri = f"http://www.wikidata.org/entity/{wikidata_data['person']}"
    name = wikidata_data.get("label")
    birth_date = wikidata_data.get("birthDate")
    death_date = wikidata_data.get("deathDate")
    occupations = wikidata_data.get("occupations", [])

    update_query = f"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX custom: <{EARTHQUAKE_MODEL}/custom/>
    
    INSERT DATA {{
       GRAPH custom:wikidata {{
            <{person_uri}> custom:closeMatch <{wikidata_uri}> .
            <{wikidata_uri}> rdfs:label "{name}" .
            {f'<{wikidata_uri}> <http://www.wikidata.org/prop/direct/P569> "{birth_date}"^^xsd:date .' if birth_date else ''}
            {f'<{wikidata_uri}> <http://www.wikidata.org/prop/direct/P570> "{death_date}"^^xsd:date .' if death_date else ''}
            {" ".join([f'<{wikidata_uri}> <http://www.wikidata.org/prop/direct/P106> "{occupation}" .' for occupation in occupations])}
        }}
    }}
    """
    sparql.setQuery(update_query)
    sparql.setMethod(POST)
    try:
        response = sparql.query()
        print(f"Updated {person_uri} with Wikidata data")
    except Exception as e:
        print(f"Error updating {person_uri} with Wikidata data: {e}")
    
def enrich_persons(cache_usage_flag):
    """Enrich each local person Wikidata data and update the endpoint."""
    persons = query_persons()
    for (p, name, birth_date, death_date) in persons:

        wikidata_data = get_wikidata_enrichment_data(name, birth_date, death_date, cache_usage_flag)
        print(f"---Enriched data (Wikidata): {wikidata_data}")
        
        if wikidata_data:
            update_person_with_wikidata_data(p, wikidata_data)

def compare_dates(date1, date2):
    try:
        year1 = int(date1.split("-")[0])
        year2 = int(date2.split("-")[0])
        return abs(year1 - year2) <= DATE_THRESHOLD
    except ValueError:
        return False

def match_persons():
    """
    Match persons after enrichment.
    Two persons are considered the same if:
      - They share the same Wikidata URI, OR
      - Their effective labels (local label plus Wikidata info) are similar enough, OR
      - Their birth and death dates are very close.
    """
    persons = query_persons_with_wikidata()
    n = len(persons)
    for i in range(n):
        p1, label1, birth1, death1, wikidata1 = persons[i]
        effective_label1 = label1
        if wikidata1:
            effective_label1 = f"{label1} ({wikidata1})"
        for j in range(i + 1, n):
            p2, label2, birth2, death2, wikidata2 = persons[j]
            effective_label2 = label2
            if wikidata2:
                effective_label2 = f"{label2} ({wikidata2})"
            
            # If both have a Wikidata URI and they are identical, we consider them the same.
            if wikidata1 and wikidata2 and (wikidata1 == wikidata2):
                print(f"Inserting owl:sameAs for persons (same Wikidata resource):")
                print(f"  {p1} ({effective_label1})")
                print(f"  {p2} ({effective_label2})")
                insert_same_as(p1, p2, "persons")
                continue

            label_similarity = fuzz.ratio(effective_label1, effective_label2)
            birth_match = birth1 and birth2 and compare_dates(birth1, birth2)
            death_match = death1 and death2 and compare_dates(death1, death2)
            name_containment = label1 in label2 or label2 in label1
            split_label1 = set(label1.split())
            split_label2 = set(label2.split())
            significant_name_difference = len(split_label1.symmetric_difference(split_label2)) > 1

            if label_similarity >= 95 or birth_match or death_match:
                if birth_match or death_match:
                    print(f"Inserting owl:sameAs for persons (date match):")
                    print(f"  {p1} ({effective_label1}, born: {birth1}, died: {death1})")
                    print(f"  {p2} ({effective_label2}, born: {birth2}, died: {death2})")
                else:
                    print(f"Inserting owl:sameAs for persons (label match):")
                    print(f"  {p1} ({effective_label1})")
                    print(f"  {p2} ({effective_label2})")
                    print(f"  Label similarity: {label_similarity}%")
                insert_same_as(p1, p2, "persons")
            elif name_containment and not significant_name_difference:
                print(f"Inserting closeMatch for persons (contained name):")
                print(f"  {p1} ({effective_label1})")
                print(f"  {p2} ({effective_label2})")
                insert_close_match(p1, p2, "persons")
            elif label_similarity >= 85:
                print(f"Inserting closeMatch for persons (name only):")
                print(f"  {p1} ({effective_label1})")
                print(f"  {p2} ({effective_label2})")
                print(f"  Label similarity: {label_similarity}%")
                insert_close_match(p1, p2, "persons")