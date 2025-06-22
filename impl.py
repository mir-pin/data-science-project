from rdflib import Graph, URIRef, Literal, RDF
from pandas import read_csv, DataFrame, read_sql
import pandas as pd 
from rdflib.plugins.stores.sparqlstore import SPARQLUpdateStore
from json import load
from sqlite3 import connect
from sparql_dataframe import get

# defining the classes
class IdentifiableEntity(object):
    def __init__(self, id):
        # if id = string
        if isinstance(id, str):
            self.id = id
        # if id = list -> string
        elif isinstance(id, list):
            self.id = ", ".join(id)
    
    def getIds(self):
        ids = []
        self.id = self.id.split(", ")
        for item in self.id:
            ids.append(item)
        return ids

class Journal(IdentifiableEntity):
    def __init__(self, id, title, languages, publisher, seal, licence, apc, hasCategory=None, hasArea=None):
        # the constructor of the superclass is explicitly recalled, so as to handle the input parameters as done in the superclass
        super().__init__(id)

        # defining the attributes
        self.title = title
        if isinstance(languages, str):
            self.languages = [languages]
        elif isinstance(languages, list):
            self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc

         # defining the relations
        if isinstance(hasCategory, str):
            self.hasCategory = [hasCategory]
        elif isinstance(hasCategory, list):
            self.hasCategory = hasCategory

        if isinstance(hasArea, str):
            self.hasArea = [hasArea]
        elif isinstance(hasArea, list):
            self.hasArea = hasArea
        
    # defining the methods of the class
    def getTitle(self):
        return self.title
    
    def getLanguages(self):
        return self.languages
    
    def getPublisher(self):
        return self.publisher
    
    def hasDOAJSeal(self):
        if self.seal == True:
            return True
        else:
            return False
    
    def getLicence(self):
        return self.licence
    
    def hasAPC(self):
        if self.apc == True:
            return True
        else:
            return False
    
    def getCategories(self):
        return self.hasCategory

    def getAreas(self):
        return self.hasArea

class Category(IdentifiableEntity):
    def __init__(self, id, quartile=None):
        super().__init__(id)
        self.quartile = quartile
        
    # defining the method of the class Category
    def getQuartile(self):
        return self.quartile

class Area(IdentifiableEntity):
    def __init__(self, id):
        super().__init__(id)


# defining the handlers
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

                # the shape of the new resources that are journals is 'https://schema.org/Periodical-<integer>'
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
        pass        # implemented in subclasses

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

        # converting the set in a list, then the list in a string
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
    
    def getById(self, id):
        con = connect(self.dbPathOrUrl)
        try:
            queries = [
                "SELECT * FROM Categories WHERE category_name = ?",
                "SELECT * FROM Areas WHERE area_name = ?",
            ]
            results = []

            for query in queries:
                rel_df = read_sql(query, con, params=(id,))     # add comma after id to make it a one-element tuple
                if not rel_df.empty:
                    results.append(rel_df)

            if results:
                df = pd.concat(results)

            # else query Journals dataframe because it could be a journal existing only in JSON
            else:
                ids = id.split(", ")
                q = " OR ".join(["identifier LIKE ?"] * len(ids))
                params = [f"%{i}%" for i in ids]
                query = f"SELECT * FROM JournalIds WHERE {q}"
                df = read_sql(query, con, params=params)

        finally:
            con.close()
            del con  # del the connection

        return df

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
        return df.drop_duplicates().reset_index(drop=True) # to avoid repetitions, but could also use SELECT DISTINCT

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
            df = read_sql(query, con, params=params)

        finally:
            con.close()
            del con  
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
        finally:
            con.close()
            del con
        return df

#defining the engines
class BasicQueryEngine(object):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        self.journalQuery = journalQuery
        self.categoryQuery = categoryQuery

    def cleanJournalHandlers(self):
        if self.journalQuery:
            self.journalQuery.clear()
            return True
        return False
      
    def cleanCategoryHandlers(self):
        if self.categoryQuery:
            self.categoryQuery.clear()
            return True
        return False
    
    def addJournalHandler(self, handler):
        if handler not in self.journalQuery:
            self.journalQuery.append(handler)
            return True
        return False
    
    def addCategoryHandler(self, handler):
        if handler not in self.categoryQuery:
            self.categoryQuery.append(handler)
            return True
        return False
    
    def addCategory(self, id):
        result = []
        all_df = []
        for handler in self.categoryQuery:
            df = handler.addCategory(id)
            if df is not None and not df.empty:
                all_df.append(df)

        if all_df:
            merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)    # to handle more handlers 
            for _, row in merged_df.iterrows():
                item = (Category(id=row["category_name"], quartile=row["quartile"]))    
                result.append((item.id, item.quartile))
        return result

    def addArea(self, id):
        result = []
        all_df = []
        for handler in self.categoryQuery:
            df = handler.addArea(id)
            if df is not None and not df.empty:
                all_df.append(df)
        
        if all_df:
            merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
            for _, row in merged_df.iterrows():
                item = (Area(id=row["area_name"]))
                result.append(item.id)
        return result
    
    def getEntityById(self, id):
        all_df = []
        for handler in self.journalQuery: 
            df = handler.getById(id)
            if df is not None and not df.empty:
                all_df.append(df)

        if all_df:
            jou_merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
            if not jou_merged_df.empty:
                row = jou_merged_df.iloc[0]
                return Journal(title = row["title"],
                            id = row["identifier"],
                            languages = row["languages"],
                            publisher = row["publisher"],
                            seal = row["seal"],
                            licence = row["licence"],
                            apc = row["apc"],
                            hasCategory = self.addCategory(id),
                            hasArea = self.addArea(id))
        
        # if nothing was returned from journalQuery, try categoryQuery 
        all_df = []
        for handler in self.categoryQuery:
            df = handler.getById(id)
            if df is not None and not df.empty:
                all_df.append(df)
        
        if not all_df:
            return None
        else:
            rel_merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        if rel_merged_df.empty:
            return None
        else:
            # if it's a journal and it exists only in the JSON file:
            if "identifier" in rel_merged_df.columns:
                if len(rel_merged_df.index) == 1:   # only one identifiers
                    row1 = rel_merged_df.iloc[0]    
                    return Journal(title = None,
                            id = row1["identifier"],
                            languages = None,
                            publisher = None,
                            seal = None,
                            licence = None,
                            apc = None,
                            hasCategory = self.addCategory(id),
                            hasArea = self.addArea(id))
                else:                              # two identifiers
                    row1 = rel_merged_df.iloc[0]
                    row2 = rel_merged_df.iloc[1]
                    return Journal(title = None,
                            id = [row1["identifier"], row2["identifier"]],
                            languages = None,
                            publisher = None,
                            seal = None,
                            licence = None,
                            apc = None,
                            hasCategory = self.addCategory(id),
                            hasArea = self.addArea(id))
            
        # inspect dataframe column to determine if Category or Area
            elif "category_name" in rel_merged_df.columns: # dealing with a Category
                row = rel_merged_df.iloc[0]
                return Category(id=row["category_name"], quartile=row.get("quartile"))
            elif "area_name" in rel_merged_df.columns: # dealing with an Area
                row = rel_merged_df.iloc[0]
                return Area(id=row["area_name"])

    def getAllJournals(self):
        result = []
        for handler in self.journalQuery:
            df = handler.getAllJournals()
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result
    
    def getJournalsWithTitle(self, partialTitle):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithTitle(partialTitle)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result
        
    def getJournalsPublishedBy(self, partialName):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsPublishedBy(partialName)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result
        
    def getJournalsWithLicense(self, licenses):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithLicense(licenses)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result

    def getJournalsWithAPC(self):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithAPC()
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result
    
    def getJournalsWithDOAJSeal(self):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithDOAJSeal()
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result

    def getAllCategories(self):
        result = []
        for handler in self.categoryQuery:
            df = handler.getAllCategories()  # returns a DataFrame
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        for _, row in merged_df.iterrows():        # _ = index
            result.append(Category(id=row["category_name"], quartile=row.get("quartile")))
        return result
        
    def getAllAreas(self):
        result = []
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        for _, row in merged_df.iterrows():
            result.append(Area(id=row["area_name"]))
        return result

    def getCategoriesWithQuartile(self, quartiles):
        result = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesWithQuartile(quartiles)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        
        for _, row in merged_df.iterrows():
            if pd.notna(row["quartile"]):
                result.append(Category(id=row["category_name"], quartile=row["quartile"]))
        return result

    def getCategoriesAssignedToAreas(self, area_ids):
        result = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesAssignedToAreas(area_ids)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            result.append(Category(id=row["category_name"]))
        return result
        
    def getAreasAssignedToCategories(self, category_ids):
        result = []
        for handler in self.categoryQuery:
            df = handler.getAreasAssignedToCategories(category_ids)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        
        for _, row in merged_df.iterrows():
            result.append(Area(id=row["area_name"]))
        return result

class FullQueryEngine(BasicQueryEngine):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        super().__init__(journalQuery, categoryQuery)

    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]):
        result = []
        identifiers = set()

        # use just the first category handler to get the database path
        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl
        
            # get journals in categories with quartiles
            with connect(path) as con:
                if not category_ids and not quartiles:
                    query = """
                    SELECT DISTINCT identifier 
                    FROM JournalIds
                    LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                    LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                    """
                else:
                    categories = list()
                    for cat in category_ids:
                        categories.append(f"'{cat}'")
                    cat_string = ", ".join(categories)
                    
                    quart = list()
                    for quartile in quartiles:
                        quart.append(f"'{quartile}'")
                    quart_string = ", ".join(quart)
                    
                    query = f"""
                    SELECT DISTINCT identifier 
                    FROM JournalIds
                    LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                    LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                    WHERE category_name IN ({cat_string}) AND quartile IN ({quart_string})
                    """
                df = read_sql(query, con)
        
                # retrieve the ids from the df
                for idx, row in df.iterrows():
                    id = df.at[idx, "identifier"]
                    identifiers.add(id)
        
        # retrieve the journals
        for id in identifiers:
            journal = super().getEntityById(id)
            if journal:
                result.append(journal)

        return result

    def getJournalsInAreasWithLicense(self, area_ids: set[str], licenses: set[str]):
        area_identifiers = set()
        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl

            # get journal ids in areas
            with connect(path) as con:
                if not area_ids:
                    query = """
                    SELECT DISTINCT identifier 
                    FROM JournalIds
                    LEFT JOIN JournalAreas ON JournalAreas.journal_id = JournalIds.journal_id
                    LEFT JOIN Areas ON Areas.area_id = JournalAreas.area_id
                    """
                    df = read_sql(query, con)
                else:
                    placeholders = ", ".join(["?"] * len(area_ids))
                    query = f"""
                    SELECT DISTINCT identifier 
                    FROM JournalIds
                    LEFT JOIN JournalAreas ON JournalAreas.journal_id = JournalIds.journal_id
                    LEFT JOIN Areas ON Areas.area_id = JournalAreas.area_id
                    WHERE area_name IN ({placeholders})
                    """
                    df = read_sql(query, con, params=list(area_ids))

                # create a set of identifiers from areas
                area_identifiers.update(df["identifier"].tolist())

        # get all licensed journals
        if not licenses:
            licensed_journals = super().getAllJournals()
        else:
            licensed_journals = super().getJournalsWithLicense(licenses)

        # filter licensed journals by area
        result = []
        for journal in licensed_journals:
            if journal.id in area_identifiers:
                result.append(journal)

        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids: set[str], category_ids: set[str], quartiles: set[str]):
        result = []
        area_identifiers = set()
        cat_identifiers = set()

        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl
            
            #  get journal ids in areas and categories with quartiles
            with connect(path) as con:
                if not area_ids:
                    query = """
                    SELECT DISTINCT identifier 
                    FROM JournalIds
                    LEFT JOIN JournalAreas ON JournalAreas.journal_id = JournalIds.journal_id
                    LEFT JOIN Areas ON Areas.area_id = JournalAreas.area_id
                    """
                    df_area = read_sql(query, con)
                else:
                    placeholders = ", ".join(["?"] * len(area_ids))
                    query = f"""
                    SELECT DISTINCT identifier 
                    FROM JournalIds
                    LEFT JOIN JournalAreas ON JournalAreas.journal_id = JournalIds.journal_id
                    LEFT JOIN Areas ON Areas.area_id = JournalAreas.area_id
                    WHERE area_name IN ({placeholders})
                    """
                    df_area = read_sql(query, con, params=list(area_ids))
            
                if not category_ids and not quartiles:
                    query = """
                    SELECT DISTINCT identifier 
                    FROM JournalIds
                    LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                    LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                    """
                else:
                    categories = list()
                    for cat in category_ids:
                        categories.append(f"'{cat}'")
                    cat_string = ", ".join(categories)
                    
                    quart = list()
                    for quartile in quartiles:
                        quart.append(f"'{quartile}'")
                    quart_string = ", ".join(quart)
                    
                    query = f"""
                    SELECT DISTINCT identifier 
                    FROM JournalIds
                    LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                    LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                    WHERE category_name IN ({cat_string}) AND quartile IN ({quart_string})
                    """
                df_cat = read_sql(query, con)
            
                # create a set of identifiers from areas
                area_identifiers.update(df_area["identifier"].tolist())
                cat_identifiers.update(df_cat["identifier"].tolist())
        
        # intersection
        cat_area_ids = cat_identifiers.intersection(area_identifiers)

        # get journals with no apc
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
                    FILTER(?apc = False)
                    }
            """
        df_apc = get(endpoint, query, True)
        apc_identifiers = set(df_apc["identifier"].tolist())
        cat_area_apc = apc_identifiers.intersection(cat_area_ids)

        # retrieve the journals
        for id in cat_area_apc:
            journal = super().getEntityById(id)
            result.append(journal)
        return result