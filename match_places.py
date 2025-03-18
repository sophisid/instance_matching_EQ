from rdflib import Namespace
from SPARQLWrapper import SPARQLWrapper, JSON, POST
import os
from fuzzywuzzy import fuzz
from utils import insert_same_as, haversine
from dotenv import load_dotenv
import requests
import json
import time
from config import sparql, GEONAMES_USERNAME

EARTHQUAKE_MODEL = Namespace("https://crm-eq.ics.forth.gr/ontology#")


GEO = Namespace("http://www.w3.org/2003/01/geo/wgs84_pos#")
COORD_THRESHOLD = 1           # km for places matching
# ------------------ GeoNames Enrichment Functions ------------------
userName = [GEONAMES_USERNAME]
cache_file = "geonames_cache.json"
# ------------------ Cache functions ------------------
def load_cache():
    """Load the GeoNames cache from a JSON file."""
    if os.path.exists(cache_file):
        with open(cache_file, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_cache(cache):
    with open(cache_file, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=4)

def get_cached_data(label, lat, lon, cache):
    key = f"{label}_{lat}_{lon}"
    return cache.get(key)

def update_cache(label, lat, lon, data, cache):
    key = f"{label}_{lat}_{lon}"
    cache[key] = data
    save_cache(cache)

def get_geonames_enrichment_data(label, lat=None, lon=None, cache_usage_flag=None, username="sophisid"):
    """
    Retrieve GeoNames data (as a dict) for enrichment.
    If coordinates are provided, try the nearby service; otherwise use the search service.
    """
    cache = load_cache()
    
    if cache_usage_flag:
        cached_data = get_cached_data(label, lat, lon, cache)
        if cached_data:
            print("Returning cached data:", cached_data)
            return cached_data
    
    if lat and lon:
        url = f"http://api.geonames.org/findNearbyPlaceNameJSON?lat={lat}&lng={lon}&username={username}"
        print(f"Retrieving GeoNames data for: {url}")
        try:
            response = requests.get(url, timeout=5)
            print(f"Response status: {response.status_code}")
            if response.status_code == 402: 
                print("All geoname user exhausted. Sleeping for 1 hour...")
                time.sleep(3600) 
                response = requests.get(url, timeout=5)
            data = response.json()
            
            if not data.get("geonames"):
                print("No geonames returned from nearby service. Response:", data)
                
            else:
                enriched = data["geonames"][0]
                print(f"Enriched data (nearby): {enriched}")
                return enriched
        except Exception as e:
            print(f"Error retrieving nearby GeoNames data: {e}")
    url = f"http://api.geonames.org/searchJSON?q={label}&maxRows=1&username={username}"
    print(f"Retrieving GeoNames data for: {url}")
    try:
        response = requests.get(url, timeout=5)
        print(f"Response status: {response.status_code}")
        if response.status_code == 402: 
            print("All geoname user exhausted. Sleeping for 1 hour...")
            time.sleep(3600)
            response = requests.get(url, timeout=5)

        data = response.json()
        if not data.get("geonames"):
            print("No geonames returned from search service. Response:", data)
            flag = True
        else:
            enriched = data["geonames"][0]
            print(f"Enriched data (search): {enriched}")
            update_cache(label, lat, lon, enriched, cache)  
            return enriched
    except Exception as e:
        print(f"Error retrieving search GeoNames data: {e}")      

    return None

def update_place_with_geonames_data(place_uri, geonames_data):    
    """
    Create a new GeoNames resource and update your endpoint so that:
      ?initialPlace owl:sameAS ?geonamesPlace .
      ?geonamesPlace has all the enrichment data.
    """
    if not geonames_data:
        return
    
    graph_name = "https://crm-eq.ics.forth.gr/ontology#/custom/geonames"

    geoname_id = geonames_data.get("geonameId")
    if geoname_id:
        geonames_uri = f"http://sws.geonames.org/{geoname_id}/"
    else:
        geonames_uri = f"http://sws.geonames.org/{geonames_data.get('name').replace(' ', '_')}"
    name = geonames_data.get("name")
    lat = geonames_data.get("lat")
    lng = geonames_data.get("lng")
    adminName1 = geonames_data.get("adminName1")
    countryName = geonames_data.get("countryName")
    update_query = f"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX gn: <http://www.geonames.org/ontology#>
    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
    PREFIX custom: <{EARTHQUAKE_MODEL}/custom/>
    INSERT DATA {{
       GRAPH custom:geonames {{
            <{place_uri}> owl:sameAs <{geonames_uri}> .
            <{geonames_uri}> gn:geonamesName "{name}" .
            <{geonames_uri}> geo:lat "{lat}"^^xsd:float .
            <{geonames_uri}> geo:long "{lng}"^^xsd:float .
            <{geonames_uri}> gn:parentFeature "{adminName1}" .
            <{geonames_uri}> gn:countryName "{countryName}" .
        }}
    }}
    """
    sparql.setQuery(update_query)
    sparql.setMethod(POST)
    sparql.query()


# ------------------ Step 1: Enrichment of Places ------------------

def query_places():
    """Query local place instances from the endpoint."""
    query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    SELECT ?p ?label ?lat ?long WHERE {
      ?p a crm:E53_Place .
      ?p rdfs:label ?label .
      OPTIONAL { ?p geo:lat ?lat . }
      OPTIONAL { ?p geo:long ?long . }
      FILTER NOT EXISTS {
        ?e <https://crm-eq.ics.forth.gr/ontology#PEQ7_has_documented_possible_epicenter_place> ?p .
      }
      FILTER NOT EXISTS {
        ?p <https://crm-eq.ics.forth.gr/ontology#PEQ7i_is__documented_possible_epicenter_place_of> ?e .
      }
    }
    """
    sparql.setQuery(query)
    sparql.setMethod("GET")
    results = sparql.query().convert()
    places = []
    for result in results["results"]["bindings"]:
        p = result["p"]["value"]
        label = result["label"]["value"]
        lat = result["lat"]["value"] if "lat" in result else None
        lon = result["long"]["value"] if "long" in result else None
        places.append((p, label, lat, lon))
    return places

def enrich_places(cache_usage_flag):
    """Enrich each local place with GeoNames data and update the endpoint."""
    places = query_places()
    for (p, label, lat, lon) in places:
        enrichment = get_geonames_enrichment_data(label, lat, lon, cache_usage_flag)
        if enrichment:
            update_place_with_geonames_data(p, enrichment)

# ------------------ Step 2: Matching of Places ------------------

def query_places_with_geonames():
    """
    Query places and also retrieve any GeoNames resource linked via owl:sameAs.
    """
    query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    SELECT ?p ?label ?lat ?long ?g WHERE {
      ?p a crm:E53_Place .
      ?p rdfs:label ?label .
      OPTIONAL { ?p geo:lat ?lat . }
      OPTIONAL { ?p geo:long ?long . }
      OPTIONAL { ?p owl:sameAs ?g .
                 FILTER(regex(str(?g), "http://sws.geonames.org/"))
      }
      FILTER NOT EXISTS {
        ?e <https://crm-eq.ics.forth.gr/ontology#PEQ7_has_documented_possible_epicenter_place> ?p .
      }
      FILTER NOT EXISTS {
        ?p <https://crm-eq.ics.forth.gr/ontology#PEQ7i_is__documented_possible_epicenter_place_of> ?e .
      }
    }
    """
    sparql.setQuery(query)
    sparql.setMethod("GET")
    results = sparql.query().convert()
    places = []
    for result in results["results"]["bindings"]:
        p = result["p"]["value"]
        label = result["label"]["value"]
        lat = result["lat"]["value"] if "lat" in result else None
        lon = result["long"]["value"] if "long" in result else None
        geonames_uri = result["g"]["value"] if "g" in result else None
        places.append((p, label, lat, lon, geonames_uri))
    return places

def match_places():
    """
    Match places after enrichment.
    Two places are considered the same if:
      - They share the same GeoNames URI, OR
      - Their effective labels (local label plus GeoNames info) are similar enough, or their coordinates are very close.
    """
    places = query_places_with_geonames()
    n = len(places)
    for i in range(n):
        p1, label1, lat1, lon1, geo1 = places[i]
        effective_label1 = label1
        if geo1:
            effective_label1 = f"{label1} ({geo1})"
        for j in range(i+1, n):
            p2, label2, lat2, lon2, geo2 = places[j]
            effective_label2 = label2
            if geo2:
                effective_label2 = f"{label2} ({geo2})"
            # If both have a GeoNames URI and they are identical, we consider them the same.
            if geo1 and geo2 and (geo1 == geo2):
                print(f"Inserting owl:sameAs for places (same GeoNames resource):")
                print(f"  {p1} ({effective_label1})")
                print(f"  {p2} ({effective_label2})")
                insert_same_as(p1, p2, "places")
                continue

            label_similarity = fuzz.ratio(effective_label1, effective_label2)
            coordinate_match = False
            distance = None
            if lat1 and lon1 and lat2 and lon2:
                try:
                    distance = haversine(float(lat1), float(lon1), float(lat2), float(lon2))
                    if distance <= COORD_THRESHOLD:
                        print(f"Distance: {distance:.3f} km")
                        coordinate_match = True
                except ValueError:
                    pass

            if label_similarity >= 95 or coordinate_match:
                if coordinate_match and distance is not None:
                    print(f"Inserting owl:sameAs for places (coordinate match):")
                    print(f"  {p1} ({effective_label1}, lat:{lat1}, lon:{lon1})")
                    print(f"  {p2} ({effective_label2}, lat:{lat2}, lon:{lon2})")
                    print(f"  Distance: {distance:.3f} km")
                else:
                    print(f"Inserting owl:sameAs for places (label match):")
                    print(f"  {p1} ({effective_label1})")
                    print(f"  {p2} ({effective_label2})")
                    print(f"  Label similarity: {label_similarity}%")
                insert_same_as(p1, p2, "places")
