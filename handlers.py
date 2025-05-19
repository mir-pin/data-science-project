from rdflib import Graph, URIRef, Literal, RDF
from pandas import read_csv
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib.namespace import XSD
from sparql_dataframe import get

import json
from json import load
import pprint
import pandas as pd
from pandas import read_sql, Series, DataFrame, merge 
from sqlite3 import connect 



class Handler(object):
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, dbPathOrUrl: str):
        self.dbPathOrUrl = dbPathOrUrl
        return True
    
class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        pass

class JournalUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path: str):
        self.path = path

        data_to_add = read_csv(path, keep_default_na=False, dtype=str, encoding="utf-8")

        # replacing the "Yes" or "No" values with booleans

        data_to_add["DOAJ Seal"] = data_to_add["DOAJ Seal"].replace({"Yes": True, "No": False})
        data_to_add["APC"] = data_to_add["APC"].replace({"Yes": True, "No": False})
        
        # setting the header of the graph database
        data_to_add.columns = ["Journal title", "id_print", "id_online", "Languages in which the journal accepts manuscripts", "Publisher", "DOAJ Seal", "Journal license", "APC"]
        data_to_add["identifier"] = data_to_add["id_print"].fillna("") + "," + data_to_add["id_online"].fillna("")
        data_to_add["identifier"] = data_to_add["identifier"].str.strip(",")
        data_to_add = data_to_add[["Journal title", "identifier", "Languages in which the journal accepts manuscripts", "Publisher", "DOAJ Seal", "Journal license", "APC"]]



        # creating an empty graph database
        graph_db = Graph()

        # defining the resources
        # class
        Journal = URIRef("https://schema.org/Periodical")

        # attributes
        title = URIRef("https://schema.org/name")
        languages = URIRef("https://schema.org/Language")
        publisher = URIRef("https://schema.org/publisher")
        identifier = URIRef("https://schema.org/identifier")
        seal = URIRef("https://www.wikidata.org/wiki/Q162919")
        licence = URIRef("https://schema.org/license")
        apc = URIRef("https://www.wikidata.org/wiki/Q15291071")
        

        base_url = "https://schema.org/"

        for idx, row in data_to_add.iterrows():
            local_id = "Periodical-" + str(idx)


            # The shape of the new resources that are journals is
            # 'https://schema.org/Periodical-<integer>'
            subj = URIRef(base_url + local_id)

            graph_db.add((subj, RDF.type, Journal))
            graph_db.add((subj, identifier, Literal(row["identifier"])))
            graph_db.add((subj, title, Literal(row["Journal title"])))
            graph_db.add((subj, languages, Literal(row["Languages in which the journal accepts manuscripts"])))
            graph_db.add((subj, publisher, Literal(row["Publisher"])))
            graph_db.add((subj, seal, Literal(row["DOAJ Seal"])))
            graph_db.add((subj, licence, Literal(row["Journal license"])))
            graph_db.add((subj, apc, Literal(row["APC"])))


        # storing data into Blazegraph

        store = SPARQLUpdateStore()
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"

        store.open((endpoint, endpoint))

        for triple in graph_db.triples((None, None, None)):
            store.add(triple)

        store.close()

        return True



class CategoryUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()  

    def pushDataToDb(self, path):
        with open(path, "r", encoding="utf-8") as f:
            json_doc = load(f)

        # 2. Lists for my DataFrames 
        journals = []
        my_categories = set()
        cat_and_quartile = []
        area_rows = set()
        cat_area_rows = []

        for journal in json_doc:

            for cat in journal.get("categories", []):
                cat_id = cat.get("id")
                quartile = cat.get("quartile")
                my_categories.add(cat_id)
                cat_and_quartile.append({
                    "category_name": cat_id,
                    "quartile": quartile
                })

                for area in journal.get("areas"):
                    area_rows.add(area)
                    cat_area_rows.append({
                        "category_name": cat_id,
                        "area_name": area
                    })

        # Journals dataframe 
        idx = 0
        for journal in json_doc:
            journal_id = "journal-" + str(idx)
            idx += 1
            for id_entry in journal.get("identifiers", []):  # For the actual ISSN and EISSN
                journals.append({
                    "journal_id": journal_id,
                    "identifier": id_entry
                }) 
                
        journals_df = DataFrame(journals)
        internal_ids = []
        for idx, row in journals_df.iterrows():
            internal_ids.append("id-" + str(idx))
        journals_df.insert(0, "internal_ids", Series(internal_ids, dtype="string"))

        # Categories dataframe
        # Generate a list of internal identifiers for categories (don't know if it is needed)
        category_internal_id = []
        categories_df = DataFrame(list(my_categories))
        for idx, row in categories_df.iterrows():
            category_internal_id.append("category-" + str(idx))
        categories_df.insert(0, "category_ids", Series(category_internal_id, dtype="string"))
        categories_df = categories_df.rename(columns={0: "category_name"})

        # Areas dataframe 
        areas_df = DataFrame({"id": list(area_rows)})
        # Generate a list of internal identifiers as a new column
        area_internal_id = [] 
        for idx, row in areas_df.iterrows():
            area_internal_id.append("area-" + str(idx))
        areas_df.insert(0, "area_ids", Series(area_internal_id, dtype="string"))
        areas_df = areas_df.rename(columns={"id": "area_name"})

        # PROBLEM !!! Handle the case in which the quartile is empty !!!
        # Category-quartile dataframe: dont' know if this is gonna work, maybe it's better just a quartile dataframe 
        cat_quartile_id =[]
        cat_quartile_df= DataFrame(cat_and_quartile)
        cat_quartile_df.drop_duplicates(inplace=True) # inplace=True to modify the original dataframe and avoid duplicates
        for idx, row in cat_quartile_df.iterrows():
            cat_quartile_id.append("cat-quartile-" + str(idx))
        cat_quartile_df.insert(0, "internal_ids", Series(cat_quartile_id, dtype="string"))

        # Category-Area dataframe QUI qualcosa non va non so cosa!!
        cat_area_df = DataFrame(cat_area_rows)
        cat_area_df.drop_duplicates(inplace=True)
        cat_area_df = cat_area_df.merge(categories_df, on="category_name")
        cat_area_df = cat_area_df.merge(areas_df, on="area_name")
        cat_area_df = cat_area_df[["category_ids", "area_ids"]]

        with connect("rel.db") as con:
            journals_df.to_sql("JournalIds", con, if_exists="replace", index=False)
            categories_df.to_sql("Categories", con, if_exists="replace", index=False)
            areas_df.to_sql("Areas", con, if_exists="replace", index=False)
            cat_quartile_df.to_sql("CategoriesQuartile", con, if_exists="replace", index=False)
            cat_area_df.to_sql("CategoriesAreas", con, if_exists="replace", index=False)
        con.commit()

