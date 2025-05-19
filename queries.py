from handlers import Handler, JournalUploadHandler
from sparql_dataframe import get


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
