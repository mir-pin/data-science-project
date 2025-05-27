from queries import QueryHandler, JournalQueryHandler, CategoryQueryHandler
from pprint import pprint
from classes import Journal, Category, Area, IdentifiableEntity

class BasicQueryEngine(object):

    def __init__(self, journalQuery=None, categoryQuery=None):
        self.categoryQuery = list()
        # for cqh in categoryQuery:
        #     self.categoryQuery.append(cqh)   
        
        self.journalQuery = list()
        # for jqh in journalQuery:
        #     self.journalQuery.append(jqh)

    def cleanJournalHandlers(self):
        result = True
        if self.journalQuery:
            self.journalQuery.clear()
        else:
            result = False
        return result
    
    def cleanCategoryHandlers(self):
        result = True
        if self.categoryQuery:
            self.categoryQuery.clear()
        else:
            result = False
        return result
    
    def addJournalHandlers(self, handler):
        result = True
        if handler not in self.journalQuery:
            self.journalQuery.append(handler)
        else:
            result = False
        return result
    
    def addCategoryHandlers(self, handler):
        result = True
        if handler not in self.categoryQuery:
            self.categoryQuery.append(handler)
        else:
            result = False
        return result
    
    def getEntityById(self, id):
        self.id = id
        for handler in self.journalQuery:
            handler = QueryHandler()
            result = handler.getById(id)
            for idx, row in result.iterrows():
                return Journal(title = row["title"],
                                  id = row["identifiers"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"],
                                  hasCategory = row["hasCategory"],
                                  hasArea = row["hasArea"])
            
        return None
    
    def getAllJournals(self):
        for handler in self.journalQuery:
            result = []
            handler = JournalQueryHandler()
            search = handler.getAllJournals()
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  id = row["identifiers"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"],
                                  hasCategory = row["hasCategory"],
                                  hasArea = row["hasArea"])
                result.append(journal)
        
        return result


        
    def getJournalsWithTitle(self, partialTitle):
        for handler in self.journalQuery:
            result = []
            handler = JournalQueryHandler()
            search = handler.getJournalsWithTitle(partialTitle)
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  id = row["identifiers"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"],
                                  hasCategory = row["hasCategory"],
                                  hasArea = row["hasArea"])
                result.append(journal)

            return result
        
        return None

        
    def getJournalsPublishedBy(self, partialName):
        for handler in self.journalQuery:
            result = []
            handler = JournalQueryHandler()
            search = handler.getJournalsPublishedBy(partialName)
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  id = row["identifiers"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"],
                                  hasCategory = row["hasCategory"],
                                  hasArea = row["hasArea"])
                result.append(journal)

            return result
        
        return None
        
    def getJournalsWithLicense(self, licenses):
        for handler in self.journalQuery:
            result = []
            handler = JournalQueryHandler()
            search = handler.getJournalsWithLicense(licenses)
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  id = row["identifiers"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"],
                                  hasCategory = row["hasCategory"],
                                  hasArea = row["hasArea"])
                result.append(journal)
            
            return result
        
        return None

    def getJournalsWithAPC(self):
        for handler in self.journalQuery:
            result = []
            handler = JournalQueryHandler()
            search = handler.getJournalsWithAPC()
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  id = row["identifiers"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"],
                                  hasCategory = row["hasCategory"],
                                  hasArea = row["hasArea"])
                result.append(journal)

            print(search)
            
            return result
        
        return None
        
    def getJournalsWithDOAJSeal(self):
        for handler in self.journalQuery:
            result = []
            handler = JournalQueryHandler()
            search = handler.getJournalsWithDOAJSeal()
            for idx, row in search.iterrows():
                journal = Journal(title = row["title"],
                                  id = row["identifiers"],
                                  languages = row["languages"],
                                  publisher = row["publisher"],
                                  seal = row["seal"],
                                  licence = row["licence"],
                                  apc = row["apc"],
                                  hasCategory = row["hasCategory"],
                                  hasArea = row["hasArea"])
                result.append(journal)
            
            return result
        
        return None
    
    def getAllCategories(self) -> list: 
        result = []
        for handler in self.categoryQuery:
            df = handler.getAllCategories()  # returns a DataFrame
            for _, row in df.iterrows():        #_ è una convenzione di Python per dire I don't care about this variable, in questo caso _ si riferisce a index
                result.append(Category(id=row['category_name'], quartile=row.get('quartile')))
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result
       

    def getAllAreas(self) -> list:
        result = []
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            for _, row in df.iterrows():    #_ è una convenzione di Python per dire I don't care about this variable, in questo caso _ si riferisce a index
                result.append(Area(id=row['area_name']))
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result

    def getCategoriesWithQuartile(self, quartiles=set()):
        result = []
        for handler in self.categoryQuery:
            df = handler.getCategoryWithQuartile(quartiles)
            for _, row in df.iterrows():
                if pd.notna(row["quartile"]):
                    result.append(Category(id=row['category_name'], quartile=row['quartile']))
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result
      

    def getCategoriesAssignedToAreas(self, area_ids=set()):
        result = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesAssignedTAreas(area_ids)
            for _, row in df.iterrows():
                result.append(Category(id=row['category_name']))
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result
        

    def getAreasAssignedToCategories(self, category_ids=set()):
        result = []
        for handler in self.categoryQuery:
            df = handler.getAreasAssignedToCategories(category_ids)
            for _, row in df.iterrows():
                result.append(Area(id=row['area_name']))
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result

