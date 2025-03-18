from SPARQLWrapper import SPARQLWrapper, JSON, POST , URLENCODED
from fuzzywuzzy import fuzz
from datetime import datetime, timedelta
import re
import os
from dateutil import parser
from utils import insert_same_as, insert_close_match, haversine 
from config import sparql



EARTHQUAKE_DATE_THRESHOLD = 1 # max year difference for earthquakes
EARTHQUAKE_COORD_THRESHOLD = 50  # km for earthquakes

# ------------------ Date Extraction & Comparison ------------------

def normalize_dates():
    query = """
    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    SELECT ?sub ?dateProperty ?dateValue WHERE {
       
            ?sub a crm:E52_Time-Span .
            ?sub ?dateProperty ?dateValue .
            FILTER (?dateProperty IN (
                crm:P82_at_some_time_within,
                crm:P82a_begin_of_the_begin,
                crm:P82b_end_of_the_end
            ))
        
    }
    """
    sparql.setQuery(query)
    sparql.setMethod("GET")
    results = sparql.query().convert()
    
    updates = []
    
    for result in results["results"]["bindings"]:
        sub = result["sub"]["value"]
        date_property = result["dateProperty"]["value"]
        date_value = result["dateValue"]["value"]

        normalized_value = normalize_date_string(date_value)

        if normalized_value:
            safe_sub = sub.replace("(", "").replace(")", "").replace("\\", "")

            update_query = f"""
            DEFINE sql:big-data-const 0
            DELETE {{
                GRAPH <http://localhost:8890/dataspace> {{
                    <{safe_sub}> <{date_property}> "{date_value}" .
                }}
            }}
            INSERT {{
                GRAPH <http://localhost:8890/dataspace> {{
                    <{safe_sub}> <{date_property}> "{normalized_value}"^^xsd:dateTime .
                }}
            }}
            WHERE {{
                GRAPH <http://localhost:8890/dataspace> {{
                    <{safe_sub}> <{date_property}> "{date_value}" .
                }}
            }}
            """
            updates.append(update_query)

    for update in updates:
        sparql.setQuery(update)
        sparql.setMethod(POST)
        sparql.query()

def normalize_date_string(value):
    """
    Normalize dates in ISO 8601 format.
    """
    try:
        dt = parser.parse(value)
        return dt.isoformat()
    except ValueError:
        if re.match(r"^\d{4}$", value):  # YYYY
            return f"{value}-01-01T00:00:00"
        elif re.match(r"^\d{4}-\d{2}$", value):  # YYYY-MM
            return f"{value}-01T00:00:00"
        elif re.match(r"^\d{4}-\d{2}-\d{2}$", value):  # YYYY-MM-DD
            return f"{value}T00:00:00"
        elif "circa" in value.lower() or "c." in value.lower():
            match = re.search(r"(\d{4})", value)
            if match:
                return f"{match.group(1)}-01-01T00:00:00"
        elif re.match(r"^\d{4}-\d{4}$", value):  # YYYY-YYYY
            years = value.split('-')
            return f"{years[0]}-01-01T00:00:00/{years[1]}-12-31T23:59:59"

    return None # cannot normalize

def extract_year(date_string):
    match = re.search(r'(\d{4})', date_string)
    if match:
        return int(match.group(1))
    return None

def extract_datetime(date_string):
    if '#' in date_string:
        date_string = date_string.split('#')[-1]
    match = re.search(r'^(\d{4}-\d{2}-\d{2}_\d{2}:\d{2})', date_string)
    if match:
        dt_str = match.group(1)
        try:
            return datetime.strptime(dt_str, "%Y-%m-%d_%H:%M")
        except ValueError:
            return None
    return None

def is_close_datetime(date1, date2, hours_threshold=3):
    dt1 = extract_datetime(date1)
    dt2 = extract_datetime(date2)
    if dt1 and dt2:
        return abs(dt1 - dt2) <= timedelta(hours=hours_threshold)
    return False

def is_year_match(date1, date2, year_threshold=1):
    dt1 = extract_datetime(date1)
    dt2 = extract_datetime(date2)
    if dt1 and dt2:
        return abs(dt1.year - dt2.year) <= year_threshold
    year1 = extract_year(date1)
    year2 = extract_year(date2)
    if year1 and year2:
        return abs(year1 - year2) <= year_threshold
    return False

def is_month_match(date1, date2, month_threshold=1):
    dt1 = extract_datetime(date1)
    dt2 = extract_datetime(date2)
    if dt1 and dt2:
        return abs(dt1.month - dt2.month) <= month_threshold
    return False

# ------------------ Matching Earthquakes ------------------

def query_earthquakes():
    query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX crm: <http://www.cidoc-crm.org/cidoc-crm/>
    PREFIX geo: <http://www.w3.org/2003/01/geo/wgs84_pos#>
    PREFIX eq: <https://crm-eq.ics.forth.gr/ontology#>
    SELECT ?eq ?label ?begin ?end ?lat ?long WHERE {
      ?eq a eq:EQ1_Earthquake .
      ?eq rdfs:label ?label .
      OPTIONAL { 
         ?eq eq:PEQ5_has_documented_possible_timespan ?ts .
         ?ts crm:P82b_end_of_the_end ?end .
      }
      OPTIONAL {
            ?eq eq:PEQ5_has_documented_possible_timespan ?ts .
            ?ts crm:P82a_begin_of_the_begin ?begin .
        }
        OPTIONAL { ?eq eq:PEQ5_has_documented_possible_timespan ?ts .
            ?ts crm:P82a_begin_of_the_begin ?begin .
            ?ts crm:P82b_end_of_the_end ?end .
        }

      OPTIONAL { ?eq <http://www.cidoc-crm.org/cidoc-crm/P4_has_time-span> ?ts .
            ?ts rdfs:label ?begin.
            ?ts rdfs:label ?end.
        }
      OPTIONAL { ?eq <http://www.cidoc-crm.org/cidoc-crm/P7_took_place_at> ?place.
        ?place owl:sameAs ?geo.
        ?geo  <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat. }
      OPTIONAL { ?eq <http://www.cidoc-crm.org/cidoc-crm/P7_took_place_at> ?place.
        ?place owl:sameAs ?geo.
        ?geo  <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long. }
    }
    """
    sparql.setQuery(query)
    sparql.setMethod("GET")
    results = sparql.query().convert()
    earthquakes = []
    for result in results["results"]["bindings"]:
        eq_id = result["eq"]["value"]
        label = result["label"]["value"]
        begin = result["begin"]["value"] if "begin" in result else None
        end = result["end"]["value"] if "end" in result else None
        lat = result["lat"]["value"] if "lat" in result else None
        lon = result["long"]["value"] if "long" in result else None
        earthquakes.append((eq_id, label, begin, end, lat, lon))
    return earthquakes

def match_earthquakes():
    earthquakes = query_earthquakes()
    n = len(earthquakes)
    for i in range(n):
        eq1, label1, begin1, end1, lat1, lon1 = earthquakes[i]
        for j in range(i+1, n):
            eq2, label2, begin2, end2, lat2, lon2 = earthquakes[j]
            if eq1 == eq2:
                continue
            label_similarity = fuzz.ratio(label1, label2)
            
            exact_date_match = False
            year_date_match = False
            month_date_match = False

            exact_date_match_end = False
            year_date_match_end = False
            month_date_match_end = False
            

            if begin1 and begin2:
                exact_date_match = is_close_datetime(begin1, begin2, hours_threshold=3)
                month_date_match = is_month_match(begin1, begin2, month_threshold=1)
                year_date_match = is_year_match(begin1, begin2, year_threshold=EARTHQUAKE_DATE_THRESHOLD)
            
            if end1 and end2:
                exact_date_match_end = is_close_datetime(end1, end2, hours_threshold=3)
                month_date_match_end = is_month_match(end1, end2, month_threshold=1)
                year_date_match_end = is_year_match(end1, end2, year_threshold=EARTHQUAKE_DATE_THRESHOLD)
            
            coord_match = False
            if lat1 and lon1 and lat2 and lon2:
                try:
                    distance = haversine(float(lat1), float(lon1), float(lat2), float(lon2))
                    if distance <= EARTHQUAKE_COORD_THRESHOLD:
                        coord_match = True
                except ValueError:
                    pass
            
            if (label_similarity >= 85 and exact_date_match) or (label_similarity >=85 and exact_date_match_end) or (coord_match and exact_date_match) or (month_date_match and coord_match) or (month_date_match_end and coord_match) or (label_similarity >= 95):
                print(f"Inserting owl:sameAs for earthquakes (exact match):") 
                print(f"  {eq1} ({label1}, begin: {begin1}, end: {end1}, {lat1}, {lon1})")
                print(f"  {eq2} ({label2}, begin: {begin2}, end:{end2}, {lat2}, {lon2})")
                insert_same_as(eq1, eq2, "earthquakes")
            elif (label_similarity >= 90 and year_date_match) or (coord_match and (year_date_match or year_date_match_end)) or ((month_date_match or month_date_match_end) and coord_match) or (month_date_match) or month_date_match_end or (label_similarity >= 80):
                print(f"Inserting closeMatch for earthquakes:")
                print(f"  {eq1} ({label1}, begin: {begin1}, end :{end1}, {lat1}, {lon1})")
                print(f"  {eq2} ({label2}, begin: {begin2}, end:{end2}, {lat2}, {lon2})")
                insert_close_match(eq1, eq2, "earthquakes")
