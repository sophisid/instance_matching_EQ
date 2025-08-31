
import re
import requests

# ------------------  wikidata Enrichment Functions ------------------

def extract_xsd_date(raw_date):
    """Convert Wikidata date format to xsd:date (YYYY-MM-DD)"""
    if not raw_date:
        return None
    return raw_date.lstrip('+').split('T')[0]  # Remove leading '+' and time portion

def query_wikidata(query):
    """Execute a SPARQL query against Wikidata."""
    url = "https://query.wikidata.org/sparql"
    headers = {"User-Agent": "MyWikidataBot/1.0"}
    response = requests.get(url, headers=headers, params={"query": query, "format": "json"})

    if response.status_code != 200:
        print(f"Error fetching data for query: {query}")
        return []

    return response.json().get("results", {}).get("bindings", [])

def extract_year(date_string):
    match = re.search(r'(\d{4})', date_string)
    if match:
        return int(match.group(1))
    return None

def get_wikidata_enrichment_data(name, birth_date=None, death_date=None, cache_usage_flag=False):
    cleaned_name = re.sub(r"\s*\(.*?\)", "", name).strip()
    probable_occupations = {
        "historian": 5, "archaeologist": 4, "geographer": 4, "seismologist": 5, "geologist": 5,
        "scholar": 3, "scientist": 3, "chronicler": 4, "writer": 2, "author": 2,
        "researcher": 3, "academic": 3, "educator": 2, "professor": 2, "teacher": 1,
    }

    
    #assuming that data in parenthesis is birth or death date
    date_match = re.search(r"\((\d{4})\)", name)
    if date_match: 
        year = date_match.group(0)
        year = year[1:-1]
        if not birth_date or not death_date:
            birth_date = year
            death_date = year
        cleaned_name = cleaned_name.replace(date_match.group(0), "").strip()
        print(f"-Date found in name assuming birth or death day: {birth_date} - {death_date}")


    query = f"""
    SELECT ?person ?personLabel ?birthDate ?deathDate ?occupationLabel WHERE {{
      ?person wdt:P31 wd:Q5;
              rdfs:label "{cleaned_name}"@en.
      OPTIONAL {{ ?person wdt:P569 ?birthDate. }} 
      OPTIONAL {{ ?person wdt:P570 ?deathDate. }}
      OPTIONAL {{ ?person wdt:P106 ?occupation. }}
      SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
    }}
    """

    results = query_wikidata(query)

    best_match = None
    best_match_score = 0
    
    occupations = []
    print(f"-[Name]: {cleaned_name} -> {len(results)} results found.")
    if results:
        occupations_set = set()
        id_bucket = []
        for result in results:
            id = result.get("person", {}).get("value", "")
            if id in id_bucket:
                if "occupationLabel" in result:
                    occupations_set.add(result["occupationLabel"]["value"])
            else:
                id_bucket.append(id)
                occupations_set = set()
                if "occupationLabel" in result:
                    occupations_set.add(result["occupationLabel"]["value"])
            match_score = sum(probable_occupations.get(occupation.lower(), 0) for occupation in occupations_set)
        
            birth_date_match = False
            death_date_match = False
            if "birthDate" in result and birth_date:
                birth_date_match = extract_year(result["birthDate"]["value"]) == extract_year(birth_date)
            if "deathDate" in result and death_date:
                death_date_match = extract_year(result["deathDate"]["value"]) == extract_year(death_date)
            
            # Increment match score for date matches
            if birth_date_match:
                match_score += 5
            if death_date_match:
                match_score += 5
            
            if match_score > best_match_score or match_score == best_match_score and len(occupations_set) > len(occupations):
                best_match = result
                best_match_score = match_score
                occupations = list(occupations_set)
                

    #[Case 1] Search by family name = whole name if no relevant occupations found 
    if not results or best_match_score == 0 or not best_match:
        print(f"--Trying family name search with whole name string being the last name...")
        whole_name_query = f"""
        SELECT ?person ?personLabel ?birthDate ?deathDate ?occupationLabel WHERE {{
          ?person wdt:P31 wd:Q5;
                  wdt:P734 ?familyName.  # Family name property
          ?familyName rdfs:label "{cleaned_name}"@en.
          OPTIONAL {{ ?person wdt:P569 ?birthDate. }} 
          OPTIONAL {{ ?person wdt:P570 ?deathDate. }}
          OPTIONAL {{ ?person wdt:P106 ?occupation. }}  
          SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
        }}
        """
        family_name_results = query_wikidata(whole_name_query)
        print(f"--[Family Name]: {cleaned_name} -> {len(family_name_results)} results found.")
        if family_name_results:
            occupations_set = set()
            id_bucket = []
            for result in family_name_results:
                id = result.get("person", {}).get("value", "")
                if id in id_bucket:
                    if "occupationLabel" in result:
                        occupations_set.add(result["occupationLabel"]["value"])
                else:
                    id_bucket.append(id)
                    occupations_set = set()
                    if "occupationLabel" in result:
                        occupations_set.add(result["occupationLabel"]["value"])
                match_score = sum(probable_occupations.get(occupation.lower(), 0) for occupation in occupations_set)

                # Check birth and death dates
                birth_date_match = False
                death_date_match = False
                if "birthDate" in result and birth_date:
                    birth_date_match = extract_year(result["birthDate"]["value"]) == extract_year(birth_date)
                if "deathDate" in result and death_date:
                    death_date_match = extract_year(result["deathDate"]["value"]) == extract_year(death_date)
                # Increment match score for date matches
                if birth_date_match:
                    match_score += 5
                if death_date_match:
                    match_score += 5

                if match_score > best_match_score or match_score == best_match_score and len(occupations_set) > len(occupations):
                    best_match = result
                    best_match_score = match_score
                    occupations = list(occupations_set)

    #[Case 2] Search by family name = last name if no results in case 1
    if not results or best_match_score == 0 or not best_match:
        
        last_name = cleaned_name.split()[-1]  # Assuming last word is the last name
        print(f"--Trying family name search assuming last word in Name string is the last name...")
        
        if last_name:
            last_name_query = f"""
            SELECT ?person ?personLabel ?birthDate ?deathDate ?occupationLabel WHERE {{
              ?person wdt:P31 wd:Q5;
                      wdt:P734 ?familyName.  # Family name property
              ?familyName rdfs:label "{last_name}"@en.
              OPTIONAL {{ ?person wdt:P569 ?birthDate. }} 
              OPTIONAL {{ ?person wdt:P570 ?deathDate. }}
              OPTIONAL {{ ?person wdt:P106 ?occupation. }}  
              SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
            }}
            """
            family_name_results = query_wikidata(last_name_query)
            print(f"--[Family Name]: {cleaned_name} -> {len(family_name_results)} results found.")
            if family_name_results:
                occupations_set = set()
                id_bucket = []
                for result in family_name_results:
                    id = result.get("person", {}).get("value", "")
                    if id in id_bucket:
                        if "occupationLabel" in result:
                            occupations_set.add(result["occupationLabel"]["value"])
                    else:
                        
                        id_bucket.append(id)
                        occupations_set = set()
                        if "occupationLabel" in result:
                            occupations_set.add(result["occupationLabel"]["value"])
                      
                    match_score = sum(probable_occupations.get(occupation.lower(), 0) for occupation in occupations_set)
                    
                    birth_date_match = False
                    death_date_match = False
                    if "birthDate" in result and birth_date:
                        birth_date_match = extract_year(result["birthDate"]["value"]) == extract_year(birth_date)
                    if "deathDate" in result and death_date:
                        death_date_match = extract_year(result["deathDate"]["value"]) == extract_year(death_date)
                    if birth_date_match:
                        match_score += 5
                    if death_date_match:
                        match_score += 5
                    if match_score > best_match_score or match_score == best_match_score and len(occupations_set) > len(occupations):
                        best_match = result
                        best_match_score = match_score
                        occupations = list(occupations_set)

        

            

    if not best_match or best_match_score == 0:
        print(f"--No relevant occupations found for {cleaned_name} or {last_name}. Skipping enrichment.")
        return {}

    person_uri = best_match["person"]["value"].split("/")[-1]  
    label = best_match.get("personLabel", {}).get("value", cleaned_name)
    birth_date = best_match["birthDate"]["value"] if "birthDate" in best_match else None
    death_date = best_match["deathDate"]["value"] if "deathDate" in best_match else None

    
    return {
        "person": person_uri,
        "label": label,
        "birthDate": birth_date,
        "deathDate": death_date,
        "occupations":  occupations, 
        "bestMatchScore": best_match_score,
        
    }

# def update_person_with_wikidata_data(person_uri, wikidata_data):
#     """
#     Create a new Wikidata resource and update the endpoint so that:
#       ?localPerson owl:sameAS ?wikidataPerson .
#       ?wikidataPerson has all the enrichment data (label, birth/death dates) and occupations.
#     """
#     if not wikidata_data:
#         return

#     wikidata_uri = f"http://www.wikidata.org/entity/{wikidata_data['person']}"
#     name = wikidata_data.get("label")
#     birth_date = wikidata_data.get("birthDate")
#     death_date = wikidata_data.get("deathDate")
#     occupations = wikidata_data.get("occupations", [])

#     update_query = f"""
#     PREFIX owl: <http://www.w3.org/2002/07/owl#>
#     PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
#     PREFIX custom: <{EARTHQUAKE_MODEL}/custom/>
    
#     INSERT DATA {{
#        GRAPH custom:wikidata {{
#             <{person_uri}> owl:sameAs <{wikidata_uri}> .
#             <{wikidata_uri}> rdfs:label "{name}" .
#             {"<"+wikidata_uri+"> <http://purl.org/dc/terms/birthDate> \""+birth_date+"\"^^xsd:date ." if birth_date else ""}
#             {"<"+wikidata_uri+"> <http://purl.org/dc/terms/deathDate> \""+death_date+"\"^^xsd:date ." if death_date else ""}
#             {" ".join([f'<{wikidata_uri}> <http://www.wikidata.org/prop/direct/P106> "{occupation}" .' for occupation in occupations])}
#         }}
#     }}
#     """

#     sparql.setQuery(update_query)
#     sparql.setMethod(POST)
#     sparql.query()
#     print(f"Updated {person_uri} with Wikidata data via {wikidata_uri}.")

    
