from rdflib import Graph, URIRef, Literal, RDF
from pandas import read_csv
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from rdflib.namespace import XSD
from sparql_dataframe import get



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
    
