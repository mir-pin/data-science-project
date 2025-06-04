# getJournalsInCategoriesWithQuartile: it returns a list of objects having class Journal containing all the journals in DOAJ that have, at least one of the input categories specified with the related quartile in Scimago Journal Rank, with no repetitions. In case the input collections of categories/quartiles are empty, it is like all categories/quartiles are actually specified.

# getJournalsInAreasWithLicense: it returns a list of objects having class Journal containing all the journals in DOAJ with at least one of the licenses specific as input, and that have at least one of the input areas specified in Scimago Journal Rank, with no repetitions. In case the input collection of areas/licenses are empty, it is like all areas/licenses are actually specified.

# getDiamondJournalsInAreasAndCategoriesWithQuartile: it returns a list of objects having class Journal containing all the journals in DOAJ that have at least one of the input categories (with the related quartiles) specified and at least one of the areas specified in Scimago Journal Rank, with no repetitions. In addition, only journals that do not have an Article Processing Charge should be considered in the result. In case the input collection of categories/quartiles/areas are empty, it is like all categories/quartiles/areas are actually specified.

from BasicQueryEngine import BasicQueryEngine
from classes import Journal, Category
from queries import JournalQueryHandler, CategoryQueryHandler, QueryHandler
from handlers import Handler, JournalUploadHandler, CategoryUploadHandler, UploadHandler
from sqlite3 import connect
from pandas import read_sql

class FullQueryEngine(BasicQueryEngine):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        super().__init__(journalQuery, categoryQuery)
    

    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]):
        result = []
        all_journals = super().getAllJournals()
        
        for journal in all_journals:
            jou_cat = journal.getCategories()
            for item in jou_cat:
                if item[0] in category_ids and item[1] in quartiles:
                    result.append(journal.title)

        return result

        # result = []
        # journal_ids = set()

        # # Use just the first category handler to get the database path
        # for handler in self.categoryQuery:
        #     path = handler.dbPathOrUrl
        #     break

        
        # with connect(path) as con:
            
        #     if not category_ids and not quartiles:
        #         query = """
        #         SELECT DISTINCT identifier 
        #         FROM JournalIds
        #         LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
        #         LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
        #         """
        #     else:
        #         categories = list()
        #         for cat in category_ids:
        #             categories.append(f"'{cat}'")
        #         cat_string = ", ".join(categories)
                
        #         quart = list()
        #         for quartile in quartiles:
        #             quart.append(f"'{quartile}'")
        #         quart_string = ", ".join(quart)
                
                
        #         query = f"""
        #         SELECT DISTINCT identifier 
        #         FROM JournalIds
        #         LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
        #         LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
        #         WHERE category_name IN ({cat_string}) AND quartile IN ({quart_string})
        #         """

        #     df = read_sql(query, con)

        # # Retrieve Journal objects by ID
        # # for _, row in df.iterrows():
        # #     journal = super().getEntityById(row["identifier"])
        # #     if journal and journal.id not in journal_ids:
        # #         result.append(journal)
        # #         journal_ids.add(journal.id)
        
        # identifiers = []
        # for idx, row in df.iterrows():
        #     identifiers.append(df.at[idx, "identifier"])

        # for handler in self.journalQuery:
        #    new_handler = BasicQueryEngine()
        #    new_handler.addJournalHandler(handler)
        #    for id in identifiers:
        #         result.append(new_handler.getEntityById(id))
        
        # return result


        # result = []
        # for handler in self.categoryQuery:
        #     path = handler.dbPathOrUrl

        
        # with connect(path) as con:
        #     q1 = list()
        #     for cat in category_ids:
        #         q1.append(f"'{cat}'")
        #     q1_string = ", ".join(q1)
            
        #     q2 = list()
        #     for quartile in quartiles:
        #         q2.append(f"'{quartile}'")
        #     q2_string = ", ".join(q2)


        #     query = f"""
        #     SELECT DISTINCT identifier 
        #     FROM JournalIds
        #     LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
        #     LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
        #     WHERE category_name IN ({q1_string}) AND quartile IN ({q2_string})
        #     """
        #     df = read_sql(query, con)
        
        # identifiers = []
        # for idx, row in df.iterrows():
        #     identifiers.append(df.at[idx, "identifier"])
        
        # for handler in self.journalQuery:
        #    new_handler = BasicQueryEngine()
        #    new_handler.addJournalHandler(handler)
        #    for id in identifiers:
        #         result.append(new_handler.getEntityById(id))


        # return result


        
    
    def getJournalsInAreasWithLicense(self, area_ids: set[str], licenses: set[str]):
        result = []
        jou_lic = []
        for licence in licenses:
            jou = super().getJournalsWithLicense({licence})
            jou_lic.extend(jou)
        all_jou_lic = super().getJournalsWithLicense(licenses)
        jou_lic.extend(all_jou_lic)    
        
        for journal in jou_lic:
            jou_area = journal.getAreas()
            for area in jou_area:
                if area in area_ids:
                    result.append(journal.title)
        return result




    #     # getJournalWithLicenses
    #     # 

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids: set[str], category_ids: set[str], quartiles: set[str]):
        result = []
        jou_no_apc = []
        all_journals = super().getAllJournals()
        for journal in all_journals:
            if journal.apc == False:
                jou_no_apc.append(journal)

        # print(len(jou_no_apc))

        
        jou_cat_list = []
        for jou in jou_no_apc:
            jou_cat = jou.getCategories()
            for item in jou_cat:
                if item[0] in category_ids and item[1] in quartiles:
                    jou_cat_list.append(jou)
        
        for jou in jou_cat_list:
            jou_area = jou.getAreas()
            for item in jou_area:
                if item in area_ids:
                    result.append(jou)

        return result
