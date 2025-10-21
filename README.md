# Part of the paper [To Automate or not to Automate the Transcription of Ancient Earthquakes: Toward a Global Knowledge Graph about Ancient Earthquakes](https://users.ics.forth.gr/~sophisid/files/2025_IJCKG_EQA2A.pdf)
#### **Project Overview**  
This project focuses on **instance matching and enrichment** for entities related to **cultural heritage (CIDOC CRM)** and **seismic data (Earthquake ontology)**. It aims to:
- **Identify duplicate instances** across different datasets.
- **Enrich existing data** with external sources like **GeoNames** and **Wikidata**.
- **Link entities using `owl:sameAs` and `custom:closeMatch`** when exact matches cannot be found.

---

## **1. Features & Functionality**  

### **Instance Matching**  
The system applies **instance matching rules** to identify duplicate entities based on:  
- **String similarity (fuzzy matching)**  
- **Date proximity comparisons**  
- **Geospatial distance calculations (Haversine formula)**  
- **Shared external identifiers (GeoNames, Wikidata)**  

### **Data Enrichment**  
The system enriches existing knowledge bases by linking entities to external sources:  
- **GeoNames** (for places)  
- **Wikidata** (for persons)  
- **Seismological databases** (planned for future)  

### **Linked Data Integration**  
- If two entities are **exactly the same**, they are linked via `owl:sameAs`.  
- If two entities are **similar but not identical**, they are linked via `custom:closeMatch`.  

---

## **2. Supported Entity Types**  

| Entity Type         | Instance Matching Criteria | External Enrichment |
|---------------------|--------------------------|----------------------|
| **E53_Place (Place)** | Label similarity, coordinate proximity, shared GeoNames URI | GeoNames (labels, population, geospatial hierarchy) |
| **E21_Person (Person)** | Name similarity, birth/death date proximity, containment of names | Wikidata (names, birth/death, occupations) |
| **EQ1_Earthquake (Earthquake)** | Date-time similarity, location proximity, label similarity | GeoNames |

---

## **3. Installation & Setup**  

### **Prerequisites**
- **Python 3.8+**
- **SPARQL Endpoint**
- **GeoNames API Key** (register at [GeoNames](https://www.geonames.org))
- **Wikidata Query Service Access**

### **Install Required Dependencies**
```bash
pip install -r requirements.txt
```

### **Environment Variables**
Create a `.env` file in the project root with:  
```
SPARQL_ENDPOINT=http://localhost:9999/blazegraph/sparql
GEONAMES_USERNAME=your_geonames_username
```

---

## **4. How to Run the Project**
The main script executes **all instance matching and enrichment steps**.  
```bash
python instance_matching.py --all --cache
```

Run specific steps:  
```bash
python instance_matching.py --person --place --eq --dates
```
- **`--all`**: Run all processes  
- **`--cache`**: Use cached enrichment data  
- **`--person`**: Match and enrich persons  
- **`--place`**: Match and enrich places  
- **`--eq`**: Match earthquakes  
- **`--dates`**: Normalize dates  

---

## **5. Matching & Enrichment Process**  

### **Step 1: Normalize Dates**
- Converts various date formats into **ISO 8601 (`xsd:dateTime`)**.

### **Step 2: Enrich Places**
- Queries **GeoNames** for:
  - Alternative place labels  
  - Geospatial hierarchy 
  - Coordinates & population  

### **Step 3: Match Places**
- Uses **fuzzy matching**, **coordinate comparisons**, and **GeoNames links**.

### **Step 4: Enrich Persons**
- Queries **Wikidata** for:
  - Birth & death dates  
  - Alternative names  
  - Occupations  

### **Step 5: Match Persons**
- Uses **name similarity**, **date proximity**, and **identifier matching**.

### **Step 6: Match Earthquakes**
- Uses **date similarity (exact, year, month match)** and **location proximity**.

---

## **6. File Structure**
```
root/
│── instance_matching.py          # controler (Main)
│── match_eq.py                   # Instance matching for earthquakes  
│── match_places.py               # Instance matching for places & enrichment  
│── utils.py                      # utility functions 
│── requirements.txt              # Python dependencies  
│── .env                          # Configuration file (SPARQL & GeoNames credentials)  
```

---

## **7. Future Improvements**
**Extend matching rules to events & activities**  
**Incorporate seismological databases for richer earthquake data**  
**Support additional enrichment sources (Getty Thesaurus, DBpedia)** 
