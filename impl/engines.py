from handlers import *
from models import *
from sqlite3 import connect
from pandas import read_sql
import pandas as pd
from sparql_dataframe import get

class BasicQueryEngine(object):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        self.journalQuery = journalQuery
        self.categoryQuery = categoryQuery

    def cleanJournalHandler(self):
        if self.journalQuery:
            self.journalQuery.clear()
            return True
        return False
      
    def cleanCategoryHandler(self):
        if self.categoryQuery:
            self.categoryQuery.clear()
            return True
        return False
    
    def addJournalHandler(self, handler):
        if handler not in self.journalQuery:
            self.journalQuery.append(handler)
            return True
        return False
    
    def addCategoryHandler(self, handler):
        if handler not in self.categoryQuery:
            self.categoryQuery.append(handler)
            return True
        return False
    
    def addCategory(self, id):
        result = []
        for handler in self.categoryQuery:
            df = handler.addCategory(id)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)       # to handle more handlers 
        if merged_df.empty:
            return None
        else:
            for _, row in merged_df.iterrows():
                item = (Category(id=row["category_name"], quartile=row["quartile"]))    
                result.append((item.id, item.quartile))
            return result

    def addArea(self, id):
        result = []
        for handler in self.categoryQuery:
            df = handler.addArea(id)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        if merged_df.empty:
            return None
        else:
            for _, row in merged_df.iterrows():
                item = (Area(id=row["area_name"]))
                result.append(item.id)
            return result
    
    def getEntityById(self, id):
        for handler in self.journalQuery and self.categoryQuery:
            df = handler.getById(id)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        
        if merged_df.empty:
            return None
        else:
            # if the entity exists in both files
            if "identifier" and "Category" and "Area" in merged_df.columns:
                row = merged_df.iloc[0]
                return Journal(title = row["title"],
                            id = row["identifier"],
                            languages = row["languages"],
                            publisher = row["publisher"],
                            seal = row["seal"],
                            licence = row["licence"],
                            apc = row["apc"],
                            hasCategory = self.addCategory(id),
                            hasArea = self.addArea(id))
            
            # it means that the entity exists only in the csv file    
            elif "identifier" in merged_df.columns:
                row = merged_df.iloc[0]
                return Journal(title = row["title"],
                            id = row["identifier"],
                            languages = row["languages"],
                            publisher = row["publisher"],
                            seal = row["seal"],
                            licence = row["licence"],
                            apc = row["apc"])
                
            # inspect dataframe column to determine if Category or Area
            elif "category_name" in merged_df.columns: # dealing with a Category
                row = merged_df.iloc[0]
                return Category(id=row["category_name"], quartile=row.get("quartile"))
            elif "area_name" in merged_df.columns: # dealing with an Area
                row = merged_df.iloc[0]
                return Area(id=row["area_name"])

    def getAllJournals(self):
        result = []
        for handler in self.journalQuery:
            df = handler.getAllJournals()
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result
    
    def getJournalsWithTitle(self, partialTitle):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithTitle(partialTitle)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result
        
    def getJournalsPublishedBy(self, partialName):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsPublishedBy(partialName)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result
        
    def getJournalsWithLicense(self, licenses):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithLicense(licenses)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result

    def getJournalsWithAPC(self):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithAPC()
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result
    
    def getJournalsWithDOAJSeal(self):
        result = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithDOAJSeal()
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            id = row["identifier"]
            journal = Journal(title = row["title"],
                              id = row["identifier"],
                              languages = row["languages"],
                              publisher = row["publisher"],
                              seal = row["seal"],
                              licence = row["licence"],
                              apc = row["apc"],
                              hasCategory = self.addCategory(id),
                              hasArea = self.addArea(id))
            result.append(journal)
        return result

    def getAllCategories(self):
        result = []
        for handler in self.categoryQuery:
            df = handler.getAllCategories()  # returns a DataFrame
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        for _, row in merged_df.iterrows():        # _ = index
            result.append(Category(id=row["category_name"], quartile=row.get("quartile")))
        return result
        
    def getAllAreas(self):
        result = []
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        for _, row in merged_df.iterrows():
            result.append(Area(id=row["area_name"]))
        return result

    def getCategoriesWithQuartile(self, quartiles):
        result = []
        for handler in self.categoryQuery:
            df = handler.getCategoryWithQuartile(quartiles)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        
        for _, row in merged_df.iterrows():
            if pd.notna(row["quartile"]):
                result.append(Category(id=row["category_name"], quartile=row["quartile"]))
        return result

    def getCategoriesAssignedToAreas(self, area_ids):
        result = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesAssignedTAreas(area_ids)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            result.append(Category(id=row["category_name"]))
        return result
        
    def getAreasAssignedToCategories(self, category_ids):
        result = []
        for handler in self.categoryQuery:
            df = handler.getAreasAssignedToCategories(category_ids)
            all_df = []
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        
        for _, row in merged_df.iterrows():
            result.append(Area(id=row["area_name"]))
        return result

# FullQueryEngine
class FullQueryEngine(BasicQueryEngine):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        super().__init__(journalQuery, categoryQuery)

    def getJournalsInCategoriesWithQuartile(self, category_ids: set[str], quartiles: set[str]):
        result = []

        # Use just the first category handler to get the database path
        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl
            break
        
        # get journals in categories with quartiles
        with connect(path) as con:
            if not category_ids and not quartiles:
                query = """
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                """
            else:
                categories = list()
                for cat in category_ids:
                    categories.append(f"'{cat}'")
                cat_string = ", ".join(categories)
                
                quart = list()
                for quartile in quartiles:
                    quart.append(f"'{quartile}'")
                quart_string = ", ".join(quart)
                
                query = f"""
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                WHERE category_name IN ({cat_string}) AND quartile IN ({quart_string})
                """
            df = read_sql(query, con)
        
        # retrieve the ids from the df
        identifiers = []
        for idx, row in df.iterrows():
            id = df.at[idx, "identifier"]
            identifiers.append(id)
        
        # retrieve the journals
        for id in identifiers:
            journal = super().getEntityById(id)
            if journal:
                result.append(journal)

        return result


    def getJournalsInAreasWithLicense(self, area_ids: set[str], licenses: set[str]):
        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl
            break

        #  get journal ids in areas
        with connect(path) as con:
            if not area_ids:
                query = """
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalAreas ON JournalAreas.journal_id = JournalIds.journal_id
                LEFT JOIN Areas ON Areas.area_id = JournalAreas.area_id
                """
                df = read_sql(query, con)
            else:
                placeholders = ", ".join(["?"] * len(area_ids))
                query = f"""
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalAreas ON JournalAreas.journal_id = JournalIds.journal_id
                LEFT JOIN Areas ON Areas.area_id = JournalAreas.area_id
                WHERE area_name IN ({placeholders})
                """
                df = read_sql(query, con, params=list(area_ids))

        # create a set of identifiers from areas
        area_identifiers = set(df["identifier"].tolist())

        # get all licensed journals
        if not licenses:
            licensed_journals = super().getAllJournals()
        else:
            licensed_journals = super().getJournalsWithLicense(licenses)

        # filter licensed journals by area
        result = []
        for journal in licensed_journals:
            if journal.id in area_identifiers:
                result.append(journal)

        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids: set[str], category_ids: set[str], quartiles: set[str]):
        result = []
        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl
            break

        #  get journal ids in areas and categories with quartiles
        with connect(path) as con:
            if not area_ids:
                query = """
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalAreas ON JournalAreas.journal_id = JournalIds.journal_id
                LEFT JOIN Areas ON Areas.area_id = JournalAreas.area_id
                """
                df_area = read_sql(query, con)
            else:
                placeholders = ", ".join(["?"] * len(area_ids))
                query = f"""
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalAreas ON JournalAreas.journal_id = JournalIds.journal_id
                LEFT JOIN Areas ON Areas.area_id = JournalAreas.area_id
                WHERE area_name IN ({placeholders})
                """
                df_area = read_sql(query, con, params=list(area_ids))
        
            if not category_ids and not quartiles:
                query = """
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                """
            else:
                categories = list()
                for cat in category_ids:
                    categories.append(f"'{cat}'")
                cat_string = ", ".join(categories)
                
                quart = list()
                for quartile in quartiles:
                    quart.append(f"'{quartile}'")
                quart_string = ", ".join(quart)
                
                query = f"""
                SELECT DISTINCT identifier 
                FROM JournalIds
                LEFT JOIN JournalCategories ON JournalCategories.journal_id = JournalIds.journal_id
                LEFT JOIN Categories ON Categories.category_id = JournalCategories.category_id
                WHERE category_name IN ({cat_string}) AND quartile IN ({quart_string})
                """
            df_cat = read_sql(query, con)
        
        # create a set of identifiers from areas
        area_identifiers = set(df_area["identifier"].tolist())
        cat_identifiers = set(df_cat["identifier"].tolist())
        cat_area_ids = cat_identifiers.intersection(area_identifiers)

        # get journals with no apc
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
                    FILTER(?apc = False)
                    }
            """
        df_apc = get(endpoint, query, True)
        apc_identifiers = set(df_apc["identifier"].tolist())
        cat_area_apc = apc_identifiers.intersection(cat_area_ids)

        # retrieve the journals
        for id in cat_area_apc:
            journal = super().getEntityById(id)
            result.append(journal)
        return result