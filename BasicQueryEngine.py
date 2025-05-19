from queries import QueryHandler, JournalQueryHandler
from pprint import pprint
from my_data_model import Journal

class BasicQueryEngine(object):

    def __init__(self, journalQuery):       # categoryQuery
        self.journalQuery = list()
        for JournalQueryHandler in journalQuery:
            self.journalQuery.append(JournalQueryHandler)

        # self.categoryQuery = list()
        # for CategoryDataQueryHandler in categoryQuery:
        #     self.categoryQuery.append(CategoryDataQueryHandler)

    def cleanJournalHandlers(self):
        self.journalQuery.clear()
        return True
    
    def addJournalHandlers(self, handler):
        self.journalQuery.append(handler)
        return True
    
    def getEntityById(self, id):
        self.id = id
        for handler in self.journalQuery:
            result = handler.getById(id)
            for idx, row in result.iterrows():
                return Journal(title = row["title"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"])
        return None
    
    # def getAllJournals(self, journals):

        
    def getJournalsWithTitle(self, partialTitle):
        for handler in self.journalQuery:
            result = []
            search = handler.getJournalsWithTitle(partialTitle)
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"])
                result.append(journal)
            return result
        
        return None

        
    def getJournalsPublishedBy(self, partialName):
        for handler in self.journalQuery:
            result = []
            search = handler.getJournalsPublishedBy(partialName)
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"])
                result.append(journal)
            return result
        
        return None
        
    def getJournalsWithLicense(self, licenses):
        for handler in self.journalQuery:
            result = []
            search = handler.getJournalsWithLicense(licenses)
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"])
                result.append(journal)
            return result
        
        return None

    def getJournalsWithAPC(self):
        for handler in self.journalQuery:
            result = []
            search = handler.getJournalsWithAPC()
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"])
                result.append(journal)
            return result
        
        return None
        
    def getJournalsWithDOAJSeal(self):
        for handler in self.journalQuery:
            result = []
            search = handler.getJournalsWithDOAJSeal()
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"])
                result.append(journal)
            return result
        
        return None




grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
# journals = JournalUploadHandler()
# journals.setDbPathOrUrl(grp_endpoint)
# journals.pushDataToDb("/Users/sara/Documents/università/magistrale (DHDK)/I. second semester/Data Science/project/data/doaj.csv")

jou_qh = JournalQueryHandler()
jou_qh.setDbPathOrUrl(grp_endpoint)

engine = BasicQueryEngine([])
engine.addJournalHandlers(jou_qh)

prova_1 = engine.getEntityById("1676-546X")
print(prova_1.seal)

# pprint(engine.getJournalsWithTitle("Pro"))
# print(engine.getJournalsWithLicense("CC BY"))
# print(engine.getJournalsWithAPC())



# Journal(title = row["title"],
#                                   id = row["identifiers"],
#                                   languages = row["languages"],
#                                   publisher = row["publisher"],
#                                   seal = row["seal"],
#                                   licence = row["licence"],
#                                   apc = row["apc"],
#                                   hasCategory = row["hasCategory"],
#                                   hasArea = row["hasArea"])