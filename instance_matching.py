from rdflib import Namespace
from SPARQLWrapper import SPARQLWrapper, JSON, POST , URLENCODED
from fuzzywuzzy import fuzz
from dotenv import load_dotenv
import os
import argparse

from person_enrichment import get_wikidata_enrichment_data   
from utils import insert_same_as, insert_close_match
from match_eq import match_earthquakes, normalize_dates
from match_places import enrich_places, match_places
from config import sparql, GEONAMES_USERNAME


# Namespaces
EARTHQUAKE_MODEL = Namespace("https://crm-eq.ics.forth.gr/ontology#")
DATE_THRESHOLD = 2            # years allowable difference for persons

# ------------------ GeoNames Enrichment Functions ------------------
userName = [GEONAMES_USERNAME]
cache_file = "geonames_cache.json"

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

def update_person_with_wikidata_data(person_uri, wikidata_data):
    global inserted_triples_count
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
    """Enrich each local person with DBpedia and Wikidata data and update the endpoint."""
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
    """Match persons based on label similarity and birth and death day proximity."""
    persons = query_persons()
    n = len(persons)
    for i in range(n):
        p1, label1, birth1, death1 = persons[i]
        for j in range(i+1, n):
            p2, label2, birth2, death2 = persons[j]
            label_similarity = fuzz.ratio(label1, label2)
            birth_match = birth1 and birth2 and compare_dates(birth1, birth2)
            death_match = death1 and death2 and compare_dates(death1, death2)
            name_containment = label1 in label2 or label2 in label1
            split_label1 = set(label1.split())
            split_label2 = set(label2.split())
            significant_name_difference = len(split_label1.symmetric_difference(split_label2)) > 1

            if label_similarity >= 85 or birth_match or death_match:
                print(f"Inserting owl:sameAs for persons:")
                print(f"  {p1} ({label1}, born: {birth1}, died: {death1})")
                print(f"  {p2} ({label2}, born: {birth2}, died: {death2})")
                print(f"  Label similarity: {label_similarity}%")
                insert_same_as(p1, p2, "persons")
            elif name_containment and not significant_name_difference:
                print(f"Inserting closeMatch for persons (contained name):")
                print(f"  {p1} ({label1}, born: {birth1}, died: {death1})")
                print(f"  {p2} ({label2}, born: {birth2}, died: {death2})")
                insert_close_match(p1, p2, "persons")
            elif label_similarity >= 85 and (birth_match or death_match):
                print(f"Inserting closeMatch for persons:")
                print(f"  {p1} ({label1}, born: {birth1}, died: {death1})")
                print(f"  {p2} ({label2}, born: {birth2}, died: {death2})")
                print(f"  Label similarity: {label_similarity}%")
                insert_close_match(p1, p2)
            elif label_similarity >= 85 or name_containment:
                print(f"Inserting closeMatch for persons (name only):")
                print(f"  {p1} ({label1}, born: {birth1}, died: {death1})")
                print(f"  {p2} ({label2}, born: {birth2}, died: {death2})")
                print(f"  Label similarity: {label_similarity}%")
                insert_close_match(p1, p2, "persons")


# ------------------ Main Steps ------------------


def main():
    global inserted_triples_count
    parser = argparse.ArgumentParser(description="Instance Matching for Places, Persons, and Earthquakes.")
    parser.add_argument("--all", action="store_true", help="Run all matching processes.")
    parser.add_argument("--cache", action="store_true", help="Use cached enrichment data.")
    parser.add_argument("--person", action="store_true", help="Run person matching.")
    parser.add_argument("--place", action="store_true", help="Run place matching.")
    parser.add_argument("--eq", action="store_true", help="Run earthquake matching.")
    parser.add_argument("--dates", action="store_true", help="Run date normalization.")

    args = parser.parse_args()
    cache_usage_flag = args.cache

    print("\nStarting instance matching process...")

    if args.all or args.dates:
        print("\nStep 1: Normalizing dates...")
        normalize_dates()

    if args.all or args.place:
        print("\nStep 2: Enriching and matching places...")
        enrich_places(cache_usage_flag)
        match_places()

    if args.all or args.person:
        print("\nStep 3: Enriching and matching persons...")
        enrich_persons(cache_usage_flag)
        # match_persons()

    if args.all or args.eq:
        print("\nStep 4: Matching earthquakes (including location proximity)...")
        match_earthquakes()
    print(f"\nTotal inserted triples: {inserted_triples_count}")

if __name__ == "__main__":
    main()

