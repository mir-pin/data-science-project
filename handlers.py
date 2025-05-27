from rdflib import Graph, URIRef, Literal, RDF
from pandas import read_csv, Series, DataFrame
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
from sqlite3 import connect 


class Handler(object):
    def __init__(self, dbPathOrUrl=None):

        # we put None to say that the attribute is optional, in case it is not yet defined
        
        if dbPathOrUrl:
            self.dbPathOrUrl = dbPathOrUrl
        else:
            self.dbPathOrUrl = ""
        

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, pathOrUrl):
        if isinstance(pathOrUrl, str):
            self.dbPathOrUrl = pathOrUrl
            return True
        return False
    
class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        pass


class JournalUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        if path:
            self.path = path

            data_to_add = read_csv(path, keep_default_na=False, dtype=str, encoding="utf-8")

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

            # store = SPARQLUpdateStore()
            # endpoint = "http://127.0.0.1:9999/blazegraph/sparql"

            # store.open((endpoint, endpoint))

            # for triple in graph_db.triples((None, None, None)):
            #     store.add(triple)

            # store.close()

            return True
        return False



# my_file = JournalUploadHandler()
# my_path = "/Users/sara/Desktop/project/data/doaj.csv"
# print(my_file.pushDataToDb(my_path))



from handlers import UploadHandler

class CategoryUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()  
   
    def pushDataToDb(self, path):
        with open(path, "r", encoding="utf-8") as f:
            json_doc = load(f)

        # 2. Lists for my DataFrames 
        journals = []
        my_categories = set()
        areas = set()
        cat_area = []
        journal_area = []
        journal_cat = []

        # Journals dataframe 
        idx = 0
        for journal in json_doc:
            journal_id = "journal-" + str(idx)
            idx += 1

            # Create lists for the DataFrames
        idx = 0
        for journal in json_doc:
            journal_id = "journal-" + str(idx)
            idx += 1
 
            for id_entry in journal.get("identifiers", []):  # For the actual ISSN and EISSN
                journals.append({
                "journal_id": journal_id,
                "identifier": id_entry
                }) 
            
            for cat in journal.get("categories", []):
                cat_id = cat.get("id")
                quartile = cat.get("quartile")
                # Journal-Category 
                journal_cat.append({
                    "journal_id": journal_id,
                    "category_name": cat_id, 
                    "quartile": quartile
                })
                # Categories
                my_categories.add(cat_id)

                for area in journal.get("areas"):
                    areas.add(area)
                    journal_area.append({
                        "journal_id": journal_id,
                        "area_name": area
                    }) 
                    cat_area.append({
                    "category_name": cat_id,
                    "area_name": area
                    })

        # Journals dataframe         
        journals_df = DataFrame(journals)
        journals_df.insert(0, "internal_id", ["id-" + str(i) for i in range(len(journals_df))])

        # Categories dataframe
        categories_df = DataFrame(list(my_categories), columns=["category_name"])
         # Generate a list of internal identifiers for categories 
        categories_df.insert(0, "category_id", ["category-" + str(i) for i in range(len(categories_df))])
        
        # Areas dataframe 
        areas_df = DataFrame(list(areas), columns=["area_name"])
        areas_df.insert(0, "area_id", ["area-" + str(i) for i in range(len(areas_df))])

        # # Journal-Area dataframe
        journal_area_df = DataFrame(journal_area, columns=["journal_id", "area_name"])
        journal_area_df = journal_area_df.merge(areas_df, on="area_name")
        journal_area_df = journal_area_df[["journal_id", "area_id"]]
        journal_area_df.drop_duplicates(inplace=True)

        # Category-Area dataframe 
        cat_area_df = DataFrame(cat_area, columns=["category_name", "area_name"])
        cat_area_df.drop_duplicates(inplace=True)
        cat_area_df = cat_area_df.merge(categories_df, on="category_name")
        cat_area_df = cat_area_df.merge(areas_df, on="area_name")
        cat_area_df = cat_area_df[["category_id", "area_id"]]

        # Journal-Category dataframe 
        journal_cat_df = DataFrame(journal_cat)
        journal_cat_df = journal_cat_df.merge(categories_df, on="category_name")
        journal_cat_df = journal_cat_df[["journal_id", "category_id", "quartile"]]

        with connect("rel.db") as con:
            journals_df.to_sql("JournalIds", con, if_exists="replace", index=False)
            categories_df.to_sql("Categories", con, if_exists="replace", index=False)
            areas_df.to_sql("Areas", con, if_exists="replace", index=False)
            journal_area_df.to_sql("JournalAreas", con, if_exists="replace", index=False)
            cat_area_df.to_sql("CategoriesAreas", con, if_exists="replace", index=False)
            journal_cat_df.to_sql("JournalCategories", con, if_exists="replace", index=False)
        con.commit()
