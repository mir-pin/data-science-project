# getJournalsInCategoriesWithQuartile: it returns a list of objects having class Journal containing all the journals in DOAJ that have, at least one of the input categories specified with the related quartile in Scimago Journal Rank, with no repetitions. In case the input collections of categories/quartiles are empty, it is like all categories/quartiles are actually specified.

# getJournalsInAreasWithLicense: it returns a list of objects having class Journal containing all the journals in DOAJ with at least one of the licenses specific as input, and that have at least one of the input areas specified in Scimago Journal Rank, with no repetitions. In case the input collection of areas/licenses are empty, it is like all areas/licenses are actually specified.

# getDiamondJournalsInAreasAndCategoriesWithQuartile: it returns a list of objects having class Journal containing all the journals in DOAJ that have at least one of the input categories (with the related quartiles) specified and at least one of the areas specified in Scimago Journal Rank, with no repetitions. In addition, only journals that do not have an Article Processing Charge should be considered in the result. In case the input collection of categories/quartiles/areas are empty, it is like all categories/quartiles/areas are actually specified.

from BasicQueryEngine import BasicQueryEngine
from classes import Journal
from queries import JournalQueryHandler, CategoryQueryHandler, QueryHandler
from handlers import Handler, JournalUploadHandler, CategoryUploadHandler, UploadHandler
from sqlite3 import connect
from pandas import read_sql

class FullQueryEngine(BasicQueryEngine):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        super().__init__(journalQuery, categoryQuery)
    

    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]):
        result = []
        journal_ids = set()

        # Use just the first category handler to get the database path
        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl
            break

        
        with connect(path) as con:
            
            if not category_ids and not quartiles:
                query = """
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                """
            else:
                categories = ', '.join(list(category_ids))
                quart = ', '.join(list(quartiles))
                query = f"""
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                WHERE category_name IN ({categories}) AND quartile in ({quart})
                """

            df = read_sql(query, con)

        # Retrieve Journal objects by ID
        for _, row in df.iterrows():
            journal = self.getEntityById(row["identifier"])
            if journal and journal.id not in journal.ids:
                result.append(journal)
                journal_ids.add(journal.id)
        
        return result

        



        


        # all_journals = super().getAllJournals()
        # 

        # for journal in all_journals:
        #     jou_cat = journal.getCategories()
        #     for item in jou_cat:
        #         if item[0] in cat_qua and item[1] in cat_qua:
        #             result.append(journal.id)

        # return result[:10]
        
    
    # def getJournalsInAreasWithLicense(self, area_ids: set[str], licenses: set[str]):
    #     result = []

    #     # getJournalWithLicenses
    #     # 

    # def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids: set[str], category_ids: set[str], quartiles: set[str]):
    #     result = []

grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
jou_qh = JournalQueryHandler()
jou_qh.setDbPathOrUrl(grp_endpoint)


rel_path = "rel.db"
cat_qh = QueryHandler()
cat_qh.setDbPathOrUrl(rel_path)

# Finally, create a advanced mashup object for asking
# about data
que = FullQueryEngine()
que.addCategoryHandler(cat_qh)
que.addJournalHandler(jou_qh)

prova = que.getJournalsInCategoriesWithQuartile({"Archeology", "History"}, {"Q1"})



# getJournalsInCategoriesWithQuartile(self, categories: set[str], quartiles: set[str]) -> list:
#     result = []
 
#     for handler in self.journalQuery:
#         all_journals = handler.getAllJournalsObjects()  # This should return a list of Journal objects
 
#         for journal in all_journals:
#             for category in journal.getCategories():
#                 category_id = category.getIds()[0]
#                 category_quartile = category.getQuartile()
 
#                 if (not categories or category_id in categories) and \
#                    (not quartiles or category_quartile in quartiles):
#                     result.append(journal)
#                     break  # we only need to match one category per journal
