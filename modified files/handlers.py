from rdflib import Graph, URIRef, Literal, RDF
from pandas import read_csv, DataFrame, read_sql
import pandas as pd 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
from sqlite3 import connect
from sparql_dataframe import get


class Handler(object):
    def __init__(self):
        self.dbPathOrUrl = "" # variable containing the path/URL of the database, initially set as an empty string
        
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, pathOrUrl):
        if isinstance(pathOrUrl, str) and pathOrUrl.strip():    # strip to check that the path is not empty
            self.dbPathOrUrl = pathOrUrl
            return True
        return False
        
class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):       # implemented in subclasses
        pass


class JournalUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        if isinstance(path, str) and path.strip():
            self.path = path

            # opening the file
            data_to_add = read_csv(path, keep_default_na=False, dtype=str, encoding="utf-8")

            # replacing the "Yes/No" values with booleans
            data_to_add["DOAJ Seal"] = data_to_add["DOAJ Seal"].replace({"Yes": True, "No": False})
            data_to_add["APC"] = data_to_add["APC"].replace({"Yes": True, "No": False})
            
            # setting the header of the graph database + combining the identifiers
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

                # The shape of the new resources that are journals is 'https://schema.org/Periodical-<integer>'
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

class CategoryUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()  
   
    def pushDataToDb(self, path):
        if isinstance(path, str) and path.strip():
            with open(path, "r", encoding="utf-8") as f:
                json_doc = load(f)

            # lists for my DataFrames 
            journals = []
            my_categories = set()
            areas = set()
            cat_area = []
            journal_area = []
            journal_cat = []

            idx = 0
            for journal in json_doc:
                journal_id = "journal-" + str(idx)
                idx += 1

                # journals dataframe
                for id_entry in journal.get("identifiers", []):  # for the actual ISSN and EISSN
                    journals.append({
                    "journal_id": journal_id,
                    "identifier": id_entry
                    }) 
                
                # journal-category
                for cat in journal.get("categories", []):
                    cat_id = cat.get("id")
                    quartile = cat.get("quartile")
                    journal_cat.append({
                        "journal_id": journal_id,
                        "category_name": cat_id, 
                        "quartile": quartile
                    })
                    # categories
                    my_categories.add(cat_id)

                    # journal-area
                    for area in journal.get("areas"):
                        areas.add(area)
                        journal_area.append({
                            "journal_id": journal_id,
                            "area_name": area
                        }) 
                        # ?
                        cat_area.append({
                        "category_name": cat_id,
                        "area_name": area
                        })

            # journals dataframe         
            journals_df = DataFrame(journals)
            journals_df.insert(0, "internal_id", ["id-" + str(i) for i in range(len(journals_df))])

            # categories dataframe
            categories_df = DataFrame(list(my_categories), columns=["category_name"])
            # Generate a list of internal identifiers for categories 
            categories_df.insert(0, "category_id", ["category-" + str(i) for i in range(len(categories_df))])
            
            # areas dataframe 
            areas_df = DataFrame(list(areas), columns=["area_name"])
            areas_df.insert(0, "area_id", ["area-" + str(i) for i in range(len(areas_df))])

            # Journal-Area dataframe
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

            # store to sqlite
            with connect(self.dbPathOrUrl) as con:
                journals_df.to_sql("JournalIds", con, if_exists="replace", index=False)
                categories_df.to_sql("Categories", con, if_exists="replace", index=False)
                areas_df.to_sql("Areas", con, if_exists="replace", index=False)
                journal_area_df.to_sql("JournalAreas", con, if_exists="replace", index=False)
                cat_area_df.to_sql("CategoriesAreas", con, if_exists="replace", index=False)
                journal_cat_df.to_sql("JournalCategories", con, if_exists="replace", index=False)
            con.commit()

            return True
        return False


class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, id):
        pass 

        # Implemented in subclasses



        # checking if the id exists in the csv file
        # endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        # query = f"""
        #     PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        #     PREFIX schema: <https://schema.org/>
        #     PREFIX wiki: <https://www.wikidata.org/wiki/>

        #     SELECT ?journal ?identifier ?title ?languages ?publisher ?seal ?licence ?apc
        #     WHERE {{ ?journal schema:identifier ?identifier ;
        #             rdf:type schema:Periodical ;
        #             schema:name ?title ;
        #             schema:Language ?languages ;
        #             schema:publisher ?publisher ;
        #             wiki:Q162919 ?seal ;
        #             schema:license ?licence ;
        #             wiki:Q15291071 ?apc .
        #             FILTER(CONTAINS(?identifier, "{id}"))
        #             }}
        #     """
        # journals_df = get(endpoint, query, True)
        
        # # checking if the id exists also in the json file
        # if not journals_df.empty:
        #     with connect(self.dbPathOrUrl) as con:
        #         query_cat = """
        #         SELECT DISTINCT category_name
        #         FROM Categories
        #         LEFT JOIN JournalCategories ON JournalCategories.category_id = Categories.category_id
        #         LEFT JOIN JournalIds ON JournalCategories.journal_id = JournalIds.journal_id
        #         WHERE identifier = ?
        #         """

        #         query_area = """
        #         SELECT DISTINCT area_name
        #         FROM Areas 
        #         LEFT JOIN JournalAreas ON JournalAreas.area_id = Areas.area_id 
        #         LEFT JOIN JournalIds ON JournalAreas.journal_id = JournalIds.journal_id
        #         WHERE identifier = ?
        #         """
 
        #     cat_df = read_sql(query_cat, con, params=(id,))
        #     area_df = read_sql(query_area, con, params=(id,))
            
        #     # if the journal exists in both files connect all the dataframes
        #     if not cat_df.empty or not area_df.empty:
        #         categories = []
        #         for idx, row in cat_df.iterrows():
        #             categories.append(cat_df.at[idx, "category_name"])      # .at to extract just the value of the cell
                
        #         areas = []
        #         for idx, row in area_df.iterrows():
        #             areas.append(area_df.at[idx, "area_name"])
                
        #         journals_df["Category"] = str(categories)
        #         journals_df["Area"] = str(areas)
        #         return journals_df
        #     else:
        #         return journals_df
        
        # # checking if the id exists in the json file
        # else:
        #     with connect(self.dbPathOrUrl) as con:
        #         queries = [
        #             "SELECT * FROM Categories WHERE category_name = ?",
        #             "SELECT * FROM Areas WHERE area_name = ?"
        #         ]

        #         results = []

        #         for query in queries:
        #             rel_df = read_sql(query, con, params=(id,)) # add comma after Id to make it a one-element tuple
        #             if not rel_df.empty:
        #                 results.append(rel_df)
        #     if results:
        #         return pd.concat(results)
        #     else:
        #         return pd.DataFrame()       # if empty still returns an empty dataframe

    # def addCategory(self, id):
    #     # split the identifiers when there are two in a string
    #     ids = []
    #     for item in id.split(", "):
    #         ids.append(item)
    #     q = " OR ".join(["INSTR(identifier, ?) > 0"] * len(ids))    # instr = detects the first occurrence of a string or a character in the other string.
        
    #     with connect(self.dbPathOrUrl) as con:
    #         query = f"""
    #             SELECT DISTINCT category_name, quartile
    #             FROM JournalCategories
    #             LEFT JOIN Categories ON JournalCategories.category_id = Categories.category_id
    #             LEFT JOIN JournalIds ON JournalIds.journal_id = JournalCategories.journal_id
    #             WHERE {q};
    #             """
    #         df = read_sql(query, con, params=ids).copy()
    #     return df

    # def addArea(self, id):
    #     ids = []
    #     for item in id.split(", "):
    #         ids.append(item)
    #     q = " OR ".join(["INSTR(identifier, ?) > 0"] * len(ids))
    
    #     with connect(self.dbPathOrUrl) as con:
    #         query = f"""
    #             SELECT DISTINCT area_name
    #             FROM JournalAreas
    #             LEFT JOIN Areas ON JournalAreas.area_id = Areas.area_id
    #             LEFT JOIN JournalIds ON JournalIds.journal_id = JournalAreas.journal_id
    #             WHERE {q};
    #             """   
    #         df = read_sql(query, con, params=ids).copy()
    #     return df

class JournalQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    def getById(self, id):
        # checking if the id exists in the csv file
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        query = f"""
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX wiki: <https://www.wikidata.org/wiki/>

            SELECT ?journal ?identifier ?title ?languages ?publisher ?seal ?licence ?apc
            WHERE {{ ?journal schema:identifier ?identifier ;
                    rdf:type schema:Periodical ;
                    schema:name ?title ;
                    schema:Language ?languages ;
                    schema:publisher ?publisher ;
                    wiki:Q162919 ?seal ;
                    schema:license ?licence ;
                    wiki:Q15291071 ?apc .
                    FILTER(CONTAINS(?identifier, "{id}"))
                    }}
            """
        journal_df = get(endpoint, query, True)
        
        return journal_df

    def getAllJournals(self):
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX wiki: <https://www.wikidata.org/wiki/>
            
            SELECT ?journal ?title ?identifier ?languages ?publisher ?seal ?licence ?apc
            WHERE {
            ?journal rdf:type schema:Periodical ;
                schema:name ?title ;
                schema:identifier ?identifier ;
                schema:Language ?languages ;
                schema:publisher ?publisher ;
                wiki:Q162919 ?seal ;
                schema:license ?licence ;
                wiki:Q15291071 ?apc .
            }
            """
        df_all_journals = get(endpoint, query, True)
        return df_all_journals
    
    def getJournalsWithTitle(self, partialTitle):
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"    
        query = f"""
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX wiki: <https://www.wikidata.org/wiki/>

            SELECT ?journal ?title ?identifier ?languages ?publisher ?seal ?licence ?apc
            WHERE {{ ?journal rdf:type schema:Periodical ;
                    schema:name ?title ;
                    schema:identifier ?identifier ;
                    schema:Language ?languages ;
                    schema:publisher ?publisher ;
                    wiki:Q162919 ?seal ;
                    schema:license ?licence ;
                    wiki:Q15291071 ?apc .
                    FILTER(CONTAINS(LCASE(STR(?title)), LCASE("{partialTitle}")))
                    }}
            """
        df_title = get(endpoint, query, True)
        return df_title
    
    def getJournalsPublishedBy(self, partialName):
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"    
        query = f"""
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX wiki: <https://www.wikidata.org/wiki/>

            SELECT ?journal ?title ?identifier ?languages ?publisher ?seal ?licence ?apc
            WHERE {{ ?journal rdf:type schema:Periodical ;
                    schema:name ?title ;
                    schema:identifier ?identifier ;
                    schema:Language ?languages ;
                    schema:publisher ?publisher ;
                    wiki:Q162919 ?seal ;
                    schema:license ?licence ;
                    wiki:Q15291071 ?apc .
                    FILTER(CONTAINS(STR(LCASE(?publisher)), LCASE("{partialName}")))
                    }}
            """
        df_publisher = get(endpoint, query, True)
        return df_publisher
    
    def getJournalsWithLicense(self, licenses):
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"

        # converting the set in a list, the list in a string
        list_licenses = []
        for licence in licenses:
            list_licenses.append(licence)
        
        filter_str = ", ".join(list_licenses)

        query = f"""
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX wiki: <https://www.wikidata.org/wiki/>

            SELECT ?journal ?title ?identifier ?languages ?publisher ?seal ?licence ?apc
            WHERE {{ ?journal rdf:type schema:Periodical ;
                    schema:name ?title ;
                    schema:identifier ?identifier ;
                    schema:Language ?languages ;
                    schema:publisher ?publisher ;
                    wiki:Q162919 ?seal ;
                    schema:license ?licence ;
                    wiki:Q15291071 ?apc .
                    FILTER(STR(?licence) IN ("{filter_str}"))
                    }}
            """
        df_licence = get(endpoint, query, True)
        return df_licence

    def getJournalsWithAPC(self):
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"    
        query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX wiki: <https://www.wikidata.org/wiki/>

            SELECT ?journal ?title ?identifier ?languages ?publisher ?seal ?licence ?apc
            WHERE { ?journal rdf:type schema:Periodical ;
                    schema:name ?title ;
                    schema:identifier ?identifier ;
                    schema:Language ?languages ;
                    schema:publisher ?publisher ;
                    wiki:Q162919 ?seal ;
                    schema:license ?licence ;
                    wiki:Q15291071 ?apc .
                    FILTER(?apc = True)
                    }
            """
        df_apc = get(endpoint, query, True)
        return df_apc

    def getJournalsWithDOAJSeal(self):
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"    
        query = """
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX wiki: <https://www.wikidata.org/wiki/>

            SELECT ?journal ?title ?identifier ?languages ?publisher ?seal ?licence ?apc
            WHERE { ?journal rdf:type schema:Periodical ;
                    schema:name ?title ;
                    schema:identifier ?identifier ;
                    schema:Language ?languages ;
                    schema:publisher ?publisher ;
                    wiki:Q162919 ?seal ;
                    schema:license ?licence ;
                    wiki:Q15291071 ?apc .
                    FILTER(?seal = True)
                    }
            """
        df_seal = get(endpoint, query, True)
        return df_seal
    
class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    # forcefylly close query: try getById
    def getById(self, id):
        con = connect(self.dbPathOrUrl)
        try:
            queries = [
                "SELECT * FROM Categories WHERE category_name = ?",
                "SELECT * FROM Areas WHERE area_name = ?",
            ]
            results = []

            for query in queries:
                rel_df = read_sql(query, con, params=(id,)).copy()
                if not rel_df.empty:
                    results.append(rel_df)

            if results:
                df = pd.concat(results)
            else:
                ids = id.split(", ")
                q = " OR ".join(["identifier LIKE ?"] * len(ids))
                params = [f"%{i}%" for i in ids]
                query = f"SELECT * FROM JournalIds WHERE {q}"
                df = read_sql(query, con, params=params).copy()

        finally:
            con.close()
            del con  # ensure GC releases the connection

        return df


    # byId originale
    # def getById(self, id):
    #     with connect(self.dbPathOrUrl) as con:
    #         queries = [
    #             "SELECT * FROM Categories WHERE category_name = ?",
    #             "SELECT * FROM Areas WHERE area_name = ?",
    #             ]
    #         results = []
            
    #         for query in queries:
    #             rel_df = read_sql(query, con, params=(id,)) # add comma after Id to make it a one-element tuple
    #             if not rel_df.empty:
    #                 results.append(rel_df)

    #         if results:
    #             df = pd.concat(results)
    #         else:
    #             ids = []
    #             for item in id.split(", "):
    #                 ids.append(item)
    #             q = " OR ".join(["INSTR(identifier, ?) > 0"] * len(ids))
    #             query = f"SELECT * FROM JournalIds WHERE {q}"

    #             df = read_sql(query, con, params=ids)
        
    #     return df 


    def getAllCategories(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT category_name FROM Categories"
            df = read_sql(query, con)
        return df 
    
    def getAllAreas(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT area_name FROM Areas"
            df = read_sql(query, con)
        return df
    
    def getCategoriesWithQuartile(self, quartiles=set()):
        with connect(self.dbPathOrUrl) as con:
            if not quartiles: # input collection empty, it's like all quartiles are specified
                query = "SELECT category_name FROM CategoriesQuartile"
                df = read_sql(query, con)
            else:
                q = ', '.join(['?'] * len(quartiles)) # placeholders
                query = f""" 
                    SELECT DISTINCT category_name, quartile 
                    FROM JournalCategories
                    LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                    WHERE quartile IN ({q}) 
                    """   
                df = read_sql(query, con, params=tuple(quartiles))
        return df
    
    def getCategoriesAssignedToAreas(self, area_ids=set()):
        with connect(self.dbPathOrUrl) as con:
            if not area_ids: # input collection empty, it's like all areas are specified
                query = "SELECT DISTINCT category_name FROM Categories"
            else:
                q = ', '.join(['?'] * len(area_ids))
                query = f"""
                SELECT DISTINCT category_name 
                FROM Categories 
                LEFT JOIN CategoriesAreas ON Categories.category_id = CategoriesAreas.category_id
                LEFT JOIN Areas ON CategoriesAreas.area_id = Areas.area_id
                WHERE area_name in ({q})
                """
            df = read_sql(query, con, params=tuple(area_ids))
        return df.drop_duplicates().reset_index(drop=True)
     
    def getAreasAssignedToCategories(self, category_ids=set()):
        with connect(self.dbPathOrUrl) as con:
            if not category_ids:
                query = "SELECT DISTINCT area_name FROM Areas"
            else:
                q = ', '.join(['?'] * len(category_ids))
                query = f"""
                SELECT DISTINCT area_name
                FROM Areas
                LEFT JOIN CategoriesAreas ON Areas.area_id = CategoriesAreas.area_id
                LEFT JOIN Categories ON CategoriesAreas.category_id = Categories.category_id
                WHERE category_name in ({q})
                """
            df = read_sql(query, con, params=tuple(category_ids))
        return df.drop_duplicates().reset_index(drop=True) # To avoid repetitions, but could also use SELECT DISTINCT

    def addCategory(self, id):
        ids = id.split(", ")
        q = " OR ".join(["identifier LIKE ?"] * len(ids))
        params = [f"%{i}%" for i in ids]   

        con = connect(self.dbPathOrUrl)
        try:
            query = f"""
                SELECT DISTINCT category_name, quartile
                FROM JournalCategories
                LEFT JOIN Categories ON JournalCategories.category_id = Categories.category_id
                LEFT JOIN JournalIds ON JournalIds.journal_id = JournalCategories.journal_id
                WHERE {q};
                """
            df = read_sql(query, con, params=params).copy() # force load
        finally:
            con.close()
            del con  # <- help GC immediately clear it
        return df


    def addArea(self, id):
        ids = id.split(", ")
        q = " OR ".join(["identifier LIKE ?"] * len(ids))
        params = [f"%{i}%" for i in ids]

        con = connect(self.dbPathOrUrl)
        try:
            query = f"""
                SELECT DISTINCT area_name
                FROM JournalAreas
                LEFT JOIN Areas ON JournalAreas.area_id = Areas.area_id
                LEFT JOIN JournalIds ON JournalIds.journal_id = JournalAreas.journal_id
                WHERE {q};
                """   
            df = read_sql(query, con, params=params)
            df = df.copy()
        finally:
            con.close()
            del con
        return df


# originali
    # def addCategory(self, id):
    #     # split the identifiers when there are two in a string
    #     ids = []
    #     for item in id.split(", "):
    #         ids.append(item)
    #     q = " OR ".join(["INSTR(identifier, ?) > 0"] * len(ids))    # instr = detects the first occurrence of a string or a character in the other string.
        
    #     with connect(self.dbPathOrUrl) as con:
    #         query = f"""
    #             SELECT DISTINCT category_name, quartile
    #             FROM JournalCategories
    #             LEFT JOIN Categories ON JournalCategories.category_id = Categories.category_id
    #             LEFT JOIN JournalIds ON JournalIds.journal_id = JournalCategories.journal_id
    #             WHERE {q};
    #             """
    #         df = read_sql(query, con, params=ids).copy()
    #     return df

    # def addArea(self, id):
    #     ids = []
    #     for item in id.split(", "):
    #         ids.append(item)
    #     q = " OR ".join(["INSTR(identifier, ?) > 0"] * len(ids))
    
    #     with connect(self.dbPathOrUrl) as con:
    #         query = f"""
    #             SELECT DISTINCT area_name
    #             FROM JournalAreas
    #             LEFT JOIN Areas ON JournalAreas.area_id = Areas.area_id
    #             LEFT JOIN JournalIds ON JournalIds.journal_id = JournalAreas.journal_id
    #             WHERE {q};
    #             """   
    #         df = read_sql(query, con, params=ids).copy()
    #     return df