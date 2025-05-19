from handlers import Handler, JournalUploadHandler
from sparql_dataframe import get

import json
from json import load
import pprint
import pandas as pd
from pandas import read_sql, Series, DataFrame, merge 
from sqlite3 import connect 

class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, id):
        endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
        query = f"""
            PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX schema: <https://schema.org/>
            PREFIX wiki: <https://www.wikidata.org/wiki/>

            SELECT ?journal ?title ?languages ?publisher ?seal ?licence ?apc
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
        df_id = get(endpoint, query, True)
        return df_id
        

class JournalQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
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
                    FILTER(CONTAINS(?licence, "{licenses}"))
                    }}
            """
        df_licence = get(endpoint, query, True)
        return df_licence
    
    def getJournalsWithAPC(self):
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
                    FILTER(?apc = True)
                    }}
            """
        df_apc = get(endpoint, query, True)
        return df_apc

    def getJournalsWithDOAJSeal(self):
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
                    FILTER(?seal = True)
                    }}
            """
        df_seal = get(endpoint, query, True)
        return df_seal


grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
# journals = JournalUploadHandler()
# journals.setDbPathOrUrl(grp_endpoint)
# journals.pushDataToDb("/Users/sara/Documents/università/magistrale (DHDK)/I. second semester/Data Science/project/data/doaj.csv")

jou_qh = JournalQueryHandler()
jou_qh.setDbPathOrUrl(grp_endpoint)

# problema encoding !!!!



class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, id):
        with connect(self.dbPathOrUrl) as con:
            queries = [
                "SELECT * FROM Categories WHERE category_name = ?",
                "SELECT * FROM Areas WHERE area_name = ?"
            ]

            for query in queries:
                df = read_sql(query, con, params=(id,)) # add comma after Id to make it a one-element tuple
                if not df.empty:
                    return df

class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()
    
    def getAllCategories(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM Categories"
            df = read_sql(query, con)
        return df 
    
    def getAllAreas(self):
        with connect(self.dbPathOrUrl) as con:
            query = "SELECT * FROM Areas"
            df = read_sql(query, con)
        return df
    
    def getCategoriesWithQuartile(self, quartiles=set()):
        with connect(self.dbPathOrUrl) as con:
            if not quartiles: # input collection empty, it's like all quartiles are specified
                query = "SELECT * FROM CategoriesQuartile"
                df = read_sql(query, con)
            else:
                q = ', '.join(['?'] * len(quartiles)) # placeholders
                query = f""" 
                    SELECT * 
                    FROM CategoriesQuartile
                    WHERE quartile IN ({q}) 
                    """   
                df = read_sql(query, con, params=tuple(quartiles))
        return df
    
    def getCategoriesAssignedToAreas(self, area_ids=set()):
        with connect(self.dbPathOrUrl) as con:
            if not area_ids: # input collection empty, it's like all areas are specified
                query = """
                SELECT category_name, area_name 
                FROM Categories
                LEFT JOIN CategoriesAreas ON Categories.category_ids = CategoriesAreas.category_ids
                LEFT JOIN Areas ON CategoriesAreas.area_ids = Areas.area_ids
                """
            else:
                q = ', '.join(['?'] * len(area_ids))
                query = f"""
                SELECT category_name, area_name 
                FROM Categories 
                LEFT JOIN CategoriesAreas ON Categories.category_ids = CategoriesAreas.category_ids
                LEFT JOIN Areas ON CategoriesAreas.area_ids = Areas.area_ids
                WHERE area_name in ({q})
                """
            df = read_sql(query, con, params=tuple(area_ids))
        return df.drop_duplicates().reset_index(drop=True)
     
    def getAreasAssignedToCategories(self, category_ids=set()):
        with connect(self.dbPathOrUrl) as con:
            if not category_ids:
                query = "SELECT area_name FROM Areas"
            else:
                q = ', '.join(['?'] * len(category_ids))
                query = f"""
                SELECT area_name, category_name
                FROM Areas
                LEFT JOIN CategoriesAreas ON Areas.area_ids = CategoriesAreas.area_ids
                LEFT JOIN Categories ON CategoriesAreas.category_ids = Categories.category_ids
                WHERE category_name in ({q})
                """
            df = read_sql(query, con, params=tuple(category_ids))
        return df.drop_duplicates().reset_index(drop=True) # To avoid repetitions, but could also use SELECT DISTINCT
