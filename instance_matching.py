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
from person_match import enrich_persons,match_persons


# ------------------ Main Steps ------------------

def main():
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
        match_persons()

    if args.all or args.eq:
        print("\nStep 4: Matching earthquakes (including location proximity)...")
        match_earthquakes()

if __name__ == "__main__":
    main()

