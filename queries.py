from handlers import Handler, CategoryUploadHandler
from sparql_dataframe import get
from sqlite3 import connect
from pandas import read_sql, Series, DataFrame, merge
import pandas as pd
from pprint import pprint
from handlers import UploadHandler, JournalUploadHandler, CategoryUploadHandler


class QueryHandler(Handler):
    def __init__(self):
        super().__init__()


    def getById(self, id):
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
        
        df_journals = get(endpoint, query, True)
        
        if not df_journals.empty:

            with connect(self.dbPathOrUrl) as con:
                query_cat = """
                SELECT DISTINCT category_name
                FROM Categories
                LEFT JOIN JournalCategories ON JournalCategories.category_id = Categories.category_id
                LEFT JOIN JournalIds ON JournalCategories.journal_id = JournalIds. journal_id
                WHERE identifier = ?
                """

                query_area = """
                SELECT DISTINCT area_name
                FROM Areas 
                LEFT JOIN JournalAreas ON JournalAreas.area_id = Areas.area_id 
                LEFT JOIN JournalIds ON JournalAreas.journal_id = JournalIds.journal_id
                WHERE identifier = ?
                """
                
            cat_df = read_sql(query_cat, con, params=(id,))
            area_df = read_sql(query_area, con, params=(id,))
            
            # if the journal exists in both files
            # connect all the dataframes
            if not cat_df.empty and not area_df.empty:
                categories = []
                for idx, row in cat_df.iterrows():
                    
                    categories.append(cat_df.at[idx, "category_name"])
                
                areas = []
                for idx, row in area_df.iterrows():
                    
                    areas.append(area_df.at[idx, "area_name"])
                
                df_journals["Category"] = str(categories)
                df_journals["Area"] = str(areas)

                
                return df_journals
            else:
                return df_journals
        
        
        else:
            with connect(self.dbPathOrUrl) as con:
                queries = [
                    "SELECT * FROM Categories WHERE category_name = ?",
                    "SELECT * FROM Areas WHERE area_name = ?"
                ]

                for query in queries:
                    df_rel = read_sql(query, con, params=(id,)) # add comma after Id to make it a one-element tuple
                    if not df_rel.empty:
                        return df_rel


        

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


