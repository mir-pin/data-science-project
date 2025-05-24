
from json import load
import pprint
import pandas as pd
from pandas import read_sql, Series, DataFrame, merge 
from sqlite3 import connect 

class IdentifiableEntity(object):
    def __init__(self, id):
        if isinstance(id, str):
            self.id = id
        elif isinstance(id, list):
            self.id = ",".join(id)

    def getIds(self):
        result = []
        for identifier in self.id:
            result.append(identifier)
        result.sort()
        return result 

class Category(IdentifiableEntity):
    def __init__(self, ids, quartile=None):
        super().__init__(ids) # Inherits id(s) from IdentifiableEntity
        self.quartile = quartile # Optional string

    def getQuartile(self):
        return self.quartile 

class Area(IdentifiableEntity):
    def __init__(self, ids):
        super().__init__(ids)
    # Inherits everything from IdentifiableEntity, no additional
    
class Journal(IdentifiableEntity):
    def __init__(self, title: str, ids: list[str], languages: list[str], publisher: str, seal: bool, licence: str, apc: bool, hasCategory, hasArea):

        # verify that some attributes have at least one item
        if not title:
            raise ValueError("Title is required")
        if not languages:
            raise ValueError("At least one language is required")
        if not licence:
            raise ValueError("Licence is required")
        
        # defining the attributes
        self.title = title

        self.languages = list()
        for language in languages:
            self.languages.append(language)
        
        self.publisher = publisher

        if seal == "Yes":
            self.seal = True
        else:
            self.seal = False

        self.licence = licence

        if apc == "Yes":
            self.apc = True
        else:
            self.apc = False

        # defining the relations
        self.hasCategory = hasCategory
        self.hasArea = hasArea

        # the constructor of the superclass is explicitly recalled, so as
        # to handle the input parameters as done in the superclass
        super().__init__(ids)


    # defining the methods of the class Journal
    def getTitle(self):
        return self.title
    
    def getLanguages(self):
        result = []
        for language in self.languages:
            result.append(language)
        result.sort()
        return result
    
    def getPublisher(self):
        return self.publisher
    
    def hasDOAJSeal(self):
        return self.seal
    
    def getLicence(self):
        return self.licence
    
    def hasAPC(self):
        return self.apc

    def getCategories(self):
        result = []
        if not self.hasCategory:
            with connect("rel.db") as con:
                query = """
                SELECT DISTINCT category_name
                FROM JournalCategories
                LEFT JOIN Categories ON JournalCategories.category_id = Categories.category_id
                LEFT JOIN JournalIds ON JournalIds.journal_id = JournalCategories.journal_id
                WHERE identifier = ?
                """
                df = read_sql(query, con, params=(self.id,))
            for _, row in df.iterrows():
                item = (Category(row["category_name"]))
                result.append(item.id)
            return result
        else:
            return self.hasCategory



class Handler(object):
    def __init__(self):
        self.dbPathOrUrl = "" # variable containing the path/URL of the database, initially set as an empty string
    
    def getDbPathOrUrl(self):
        return self.dbPathOrUrl
    
    def setDbPathOrUrl(self, pathOrUrl): # set a new path/URL
        if isinstance(pathOrUrl, str):
            self.dbPathOrUrl = pathOrUrl
            return True
        return False 



class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path): 
        pass 



class CategoryUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()  
   
    def pushDataToDb(self, path):
        with open(path, "r", encoding="utf-8") as f:
            json_doc = load(f)

        # 2. Lists for my DataFrames 
        journals = []
        my_categories = set()
        areas = set()
        cat_area = []
        journal_area = []
        journal_cat = []

        # Journals dataframe 
        idx = 0
        for journal in json_doc:
            journal_id = "journal-" + str(idx)
            idx += 1

            # Create lists for the DataFrames
        idx = 0
        for journal in json_doc:
            journal_id = "journal-" + str(idx)
            idx += 1
 
            for id_entry in journal.get("identifiers", []):  # For the actual ISSN and EISSN
                journals.append({
                "journal_id": journal_id,
                "identifier": id_entry
                }) 
            
            for cat in journal.get("categories", []):
                cat_id = cat.get("id")
                quartile = cat.get("quartile")
                # Journal-Category 
                journal_cat.append({
                    "journal_id": journal_id,
                    "category_name": cat_id, 
                    "quartile": quartile
                })
                # Categories
                my_categories.add(cat_id)

                for area in journal.get("areas"):
                    areas.add(area)
                    journal_area.append({
                        "journal_id": journal_id,
                        "area_name": area
                    }) 
                    cat_area.append({
                    "category_name": cat_id,
                    "area_name": area
                    })

        # Journals dataframe         
        journals_df = DataFrame(journals)
        journals_df.insert(0, "internal_id", ["id-" + str(i) for i in range(len(journals_df))])

        # Categories dataframe
        categories_df = DataFrame(list(my_categories), columns=["category_name"])
         # Generate a list of internal identifiers for categories 
        categories_df.insert(0, "category_id", ["category-" + str(i) for i in range(len(categories_df))])
        
        # Areas dataframe 
        areas_df = DataFrame(list(areas), columns=["area_name"])
        areas_df.insert(0, "area_id", ["area-" + str(i) for i in range(len(areas_df))])

        # # Journal-Area dataframe
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

        with connect(self.dbPathOrUrl) as con:
            journals_df.to_sql("JournalIds", con, if_exists="replace", index=False)
            categories_df.to_sql("Categories", con, if_exists="replace", index=False)
            areas_df.to_sql("Areas", con, if_exists="replace", index=False)
            journal_area_df.to_sql("JournalAreas", con, if_exists="replace", index=False)
            cat_area_df.to_sql("CategoriesAreas", con, if_exists="replace", index=False)
            journal_cat_df.to_sql("JournalCategories", con, if_exists="replace", index=False)
        con.commit()



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

class JournalQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()


# FullQueryEngine and BasicQueryEngine
class BasicQueryEngine(object):
    def __init__(self, journalQuery=None, categoryQuery=None): # default values
        self.journalQuery = list()
        self.categoryQuery = list() 

    def cleanJournalHandlers(self):
        result = True
        if not self.journalQuery:
            result = False
        else:
            self.journalQuery.clear()
        return result

    def cleanCategoryHandlers(self):
        result = True
        if not self.categoryQuery:
            result = False 
        else:
            self.categoryQuery.clear() 
        return result

    def addJournalHandler(self, handler):
        result = True
        if handler not in self.journalQuery:
            self.journalQuery.append(handler)
        else:
            result = False
        return result
    
    def addCategoryHandler(self, handler):
        result = True
        if handler not in self.categoryQuery:
            self.categoryQuery.append(handler)
        else: 
            result = False 
        return result

    def getEntityById(self, id):
        pass

    def getAllJournals(self):
        pass

    def getJournalsWithTitle(self, partialTitle):
        pass

    def getJournalsPublishedBy(self, partialName):
        pass

    def getJournalsWithLicense(self, licenses=set()):
        pass

    def JournalsWithAPC(self):
        pass

    def JournalsWithDOAJSeal(self):
        pass

    def getAllCategories(self) -> list: 
        result = []
        for handler in self.categoryQuery:
            df = handler.getAllCategories()  # returns a DataFrame
            for _, row in df.iterrows():        #_ è una convenzione di Python per dire I don't care about this variable, in questo caso _ si riferisce a index
                result.append(Category(ids=row['category_name'], quartile=row.get('quartile')))
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result
       

    def getAllAreas(self) -> list:
        result = []
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            for _, row in df.iterrows():    #_ è una convenzione di Python per dire I don't care about this variable, in questo caso _ si riferisce a index
                result.append(Area(ids=row['area_name']))
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
                    result.append(Category(ids=row['category_name'], quartile=row['quartile']))
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result
      

    def getCategoriesAssignedToAreas(self, area_ids=set()):
        result = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesAssignedTAreas(area_ids)
            for _, row in df.iterrows():
                result.append(Category(ids=row['category_name'])
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result
        

    def getAreasAssignedToCategories(self, category_ids=set()):
        result = []
        for handler in self.categoryQuery:
            df = handler.getAreasAssignedToCategories(category_ids):
            for _, row in df.iterrows():
                result.append(Area(area=row['area_name'])
        second_result = []
        for item in result:
            second_result.append(item.id)
        return second_result


class FullQueryEngine(BasicQueryEngine):
    def __init__(self):
        super().__init__()

    def getJournalsInCategoriesWithQuartile():
        pass

    def getJournalsInAreasWithLicense():
        pass

    def getDiamondJournalsInAreasAndCategoriesWithQuartile():
        pass 


