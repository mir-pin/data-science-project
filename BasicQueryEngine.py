from queries import QueryHandler, JournalQueryHandler
from pprint import pprint
from my_data_model import Journal

class BasicQueryEngine(object):

    def __init__(self, journalQuery, categoryQuery):
        self.journalQuery = list()
        for jqh in journalQuery:
            self.journalQuery.append(jqh)

        self.categoryQuery = list()
        for cqh in categoryQuery:
            self.categoryQuery.append(cqh)

    def cleanJournalHandlers(self):
        result = True
        if not self.journalQuery:
            result = False
        else:
            self.journalQuery.clear()
        return result
    
    def addJournalHandlers(self, handler):
        result = True
        if handler not in self.journalQuery:
            self.journalQuery.append(handler)
        else:
            result = False
        return result
    
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
