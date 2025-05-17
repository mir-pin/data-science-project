import json
from json import load
import pprint
import pandas as pd
from pandas import read_sql, Series, DataFrame, merge 
from sqlite3 import connect 

class IdentifiableEntity(object):
    def __init__(self, ids):
        if isinstance(ids, str): # isinstance checks wheter a value is of a specific type
            self.ids = [ids]     # it guarantees that self.ids is always a list 
        elif isinstance(ids, list):
            self.ids = ids
        else:
            raise ValueError("it must be a string or list of strings")
        
    def getIds(self):
        return self.ids 

class Category(IdentifiableEntity):
    def __init__(self, ids, quartile=None):
        super().__init__(ids) # Inherits id(s) from IdentifiableEntity
        self.quartile = quartile # Optional string

    def getQuartile(self):
        return self.quartile 

class Area(IdentifiableEntity):
    pass # Inherits everything from IdentifiableEntity, no additional
    
class Journal(IdentifiableEntity):
    pass



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
        cat_and_quartile = []
        area_rows = set()
        cat_area_rows = []

        for journal in json_doc:

            for cat in journal.get("categories", []):
                cat_id = cat.get("id")
                quartile = cat.get("quartile")
                my_categories.add(cat_id)
                cat_and_quartile.append({
                    "category_name": cat_id,
                    "quartile": quartile
                })

                for area in journal.get("areas"):
                    area_rows.add(area)
                    cat_area_rows.append({
                        "category_name": cat_id,
                        "area_name": area
                    })

        # Journals dataframe 
        idx = 0
        for journal in json_doc:
            journal_id = "journal-" + str(idx)
            idx += 1
            for id_entry in journal.get("identifiers", []):  # For the actual ISSN and EISSN
                journals.append({
                    "journal_id": journal_id,
                    "identifier": id_entry
                }) 
                
        journals_df = DataFrame(journals)
        internal_ids = []
        for idx, row in journals_df.iterrows():
            internal_ids.append("id-" + str(idx))
        journals_df.insert(0, "internal_ids", Series(internal_ids, dtype="string"))

        # Categories dataframe
        # Generate a list of internal identifiers for categories (don't know if it is needed)
        category_internal_id = []
        categories_df = DataFrame(list(my_categories))
        for idx, row in categories_df.iterrows():
            category_internal_id.append("category-" + str(idx))
        categories_df.insert(0, "category_ids", Series(category_internal_id, dtype="string"))
        categories_df = categories_df.rename(columns={0: "category_name"})

        # Areas dataframe 
        areas_df = DataFrame({"id": list(area_rows)})
        # Generate a list of internal identifiers as a new column
        area_internal_id = [] 
        for idx, row in areas_df.iterrows():
            area_internal_id.append("area-" + str(idx))
        areas_df.insert(0, "area_ids", Series(area_internal_id, dtype="string"))
        areas_df = areas_df.rename(columns={"id": "area_name"})

        # PROBLEM !!! Handle the case in which the quartile is empty !!!
        # Category-quartile dataframe: dont' know if this is gonna work, maybe it's better just a quartile dataframe 
        cat_quartile_id =[]
        cat_quartile_df= DataFrame(cat_and_quartile)
        cat_quartile_df.drop_duplicates(inplace=True) # inplace=True to modify the original dataframe and avoid duplicates
        for idx, row in cat_quartile_df.iterrows():
            cat_quartile_id.append("cat-quartile-" + str(idx))
        cat_quartile_df.insert(0, "internal_ids", Series(cat_quartile_id, dtype="string"))

        # Category-Area dataframe QUI qualcosa non va non so cosa!!
        cat_area_df = DataFrame(cat_area_rows)
        cat_area_df.drop_duplicates(inplace=True)
        cat_area_df = cat_area_df.merge(categories_df, on="category_name")
        cat_area_df = cat_area_df.merge(areas_df, on="area_name")
        cat_area_df = cat_area_df[["category_ids", "area_ids"]]

        with connect("rel.db") as con:
            journals_df.to_sql("JournalIds", con, if_exists="replace", index=False)
            categories_df.to_sql("Categories", con, if_exists="replace", index=False)
            areas_df.to_sql("Areas", con, if_exists="replace", index=False)
            cat_quartile_df.to_sql("CategoriesQuartile", con, if_exists="replace", index=False)
            cat_area_df.to_sql("CategoriesAreas", con, if_exists="replace", index=False)
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

class JournalQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()


# FullQueryEngine and BasicQueryEngine
class BasicQueryEngine(object):
    def __init__(self):
        self.journalQuery = JournalQueryHandler 
        self.categoryQuery = CategoryQueryHandler 

    def cleanJournalHandlers(self):
        pass

    def cleanCategoryHandlers(self):
        pass

    def addJournalHandler(self, handler=JournalQueryHandler):
        pass
    
    def addCategoryHandler(self, handler=CategoryQueryHandler):
        pass

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

    def getAllCategories(self):
        pass

    def getAllAreas(self):
        pass

    def getCategoriesWithQuartile(self, quartiles=set()):
        pass

    def getCategoriesAssignedToAreas(self, area_ids=set()):
        pass

    def getAreasAssignedToCategories(self, category_ids=set()):
        pass


class FullQueryEngine(BasicQueryEngine):
    def __init__(self):
        super().__init__()

    def getJournalsInCategoriesWithQuartile():
        pass

    def getJournalsInAreasWithLicense():
        pass

    def getDiamondJournalsInAreasAndCategoriesWithQuartile():
        pass 


