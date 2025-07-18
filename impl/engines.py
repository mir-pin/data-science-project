from models import Journal, Category, Area
from sqlite3 import connect
from pandas import read_sql
import pandas as pd
from sparql_dataframe import get

class BasicQueryEngine(object):
    def __init__(self, journalQuery=[], categoryQuery=[]):
        self.journalQuery = journalQuery
        self.categoryQuery = categoryQuery

    def cleanJournalHandlers(self):
        if self.journalQuery:
            self.journalQuery.clear()
            return True
        return False
      
    def cleanCategoryHandlers(self):
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
        all_df = []
        for handler in self.categoryQuery:
            df = handler.addCategory(id)
            if df is not None and not df.empty:
                all_df.append(df)

        if all_df:
            merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)    # to handle more handlers
            for _, row in merged_df.iterrows():
                item = Category(id=row["category_name"], quartile=row["quartile"])    
                result.append(item)
        return result

    def addArea(self, id):
        result = []
        all_df = []
        for handler in self.categoryQuery:
            df = handler.addArea(id)
            if df is not None and not df.empty:
                all_df.append(df)
        
        if all_df:
            merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
            for _, row in merged_df.iterrows():
                item = Area(id=row["area_name"])
                result.append(item)
        return result
    
    def getEntityById(self, id):
        all_df = []
        for handler in self.journalQuery: 
            df = handler.getById(id)
            if df is not None and not df.empty:
                all_df.append(df)

        if all_df:
            jou_merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
            if not jou_merged_df.empty:
                row = jou_merged_df.iloc[0]
                return Journal(title = row["title"],
                            id = row["identifier"],
                            languages = row["languages"],
                            publisher = row["publisher"],
                            seal = row["seal"],
                            licence = row["licence"],
                            apc = row["apc"],
                            hasCategory = self.addCategory(id),
                            hasArea = self.addArea(id))
        
        # If nothing was returned from journalQuery, try categoryQuery 
        all_df = []
        for handler in self.categoryQuery:
            df = handler.getById(id)
            if df is not None and not df.empty:
                all_df.append(df)
        
        if not all_df:
            return None
        else:
            rel_merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        if rel_merged_df.empty:
            return None
        else:
            # if it's a journal and it exists only in the JSON file:
            if "identifier" in rel_merged_df.columns:
                if len(rel_merged_df.index) == 1:   # only one identifiers
                    row1 = rel_merged_df.iloc[0]    
                    return Journal(title = None,
                            id = row1["identifier"],
                            languages = None,
                            publisher = None,
                            seal = None,
                            licence = None,
                            apc = None,
                            hasCategory = self.addCategory(id),
                            hasArea = self.addArea(id))
                else:                              # two identifiers
                    row1 = rel_merged_df.iloc[0]
                    row2 = rel_merged_df.iloc[1]
                    return Journal(title = None,
                            id = [row1["identifier"], row2["identifier"]],
                            languages = None,
                            publisher = None,
                            seal = None,
                            licence = None,
                            apc = None,
                            hasCategory = self.addCategory(id),
                            hasArea = self.addArea(id))
            
        # inspect dataframe column to determine if Category or Area
            elif "category_name" in rel_merged_df.columns: # dealing with a Category
                row = rel_merged_df.iloc[0]
                return Category(id=row["category_name"], quartile=row.get("quartile"))
            elif "area_name" in rel_merged_df.columns: # dealing with an Area
                row = rel_merged_df.iloc[0]
                return Area(id=row["area_name"])

    def getAllJournals(self):
        result = []
        all_df = []
        for handler in self.journalQuery:
            df = handler.getAllJournals()
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
        all_df = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithTitle(partialTitle)
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
        all_df = []
        for handler in self.journalQuery:
            df = handler.getJournalsPublishedBy(partialName)
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
        all_df = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithLicense(licenses)
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
        all_df = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithAPC()
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
        all_df = []
        for handler in self.journalQuery:
            df = handler.getJournalsWithDOAJSeal()
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
        all_df = []
        for handler in self.categoryQuery:
            df = handler.getAllCategories()  # returns a DataFrame
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        for _, row in merged_df.iterrows():        # _ = index
            result.append(Category(id=row["category_name"], quartile=row.get("quartile")))
        return result
        
    def getAllAreas(self):
        result = []
        all_df = []
        for handler in self.categoryQuery:
            df = handler.getAllAreas()
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        for _, row in merged_df.iterrows():
            result.append(Area(id=row["area_name"]))
        return result

    def getCategoriesWithQuartile(self, quartiles):
        result = []
        all_df = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesWithQuartile(quartiles)
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)
        
        for _, row in merged_df.iterrows():
            if pd.notna(row["quartile"]):
                result.append(Category(id=row["category_name"], quartile=row["quartile"]))
        return result

    def getCategoriesAssignedToAreas(self, area_ids):
        result = []
        all_df = []
        for handler in self.categoryQuery:
            df = handler.getCategoriesAssignedToAreas(area_ids)
            all_df.append(df)
        merged_df = pd.concat(all_df).drop_duplicates().reset_index(drop = True)

        for _, row in merged_df.iterrows():
            result.append(Category(id=row["category_name"]))
        return result
        
    def getAreasAssignedToCategories(self, category_ids):
        result = []
        all_df = []
        for handler in self.categoryQuery:
            df = handler.getAreasAssignedToCategories(category_ids)
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
        identifiers = set()

        # Use just the first category handler to get the database path
        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl
        
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
                for idx, row in df.iterrows():
                    id = df.at[idx, "identifier"]
                    identifiers.add(id)
        
        # retrieve the journals
        for id in identifiers:
            journal = super().getEntityById(id)
            if journal and journal not in result:
                result.append(journal)

        return result


    def getJournalsInAreasWithLicense(self, area_ids: set[str], licenses: set[str]):
        area_identifiers = []
        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl

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
                area_identifiers.extend(df["identifier"].tolist())

        # get all licensed journals
        licensed_journals = []
        if not licenses:
            licensed_journals = super().getAllJournals()
        else:
            for license in licenses:
                jou = super().getJournalsWithLicense({license})
                licensed_journals.extend(jou)

        # filter licensed journals by area
        seen = set()
        result = []

        for journal in licensed_journals:
            if journal not in seen:
                jou_ids = journal.getIds()
                for jou_id in jou_ids:
                    splitted_ids = jou_id.split(",")
                    if any(id.strip() in area_identifiers for id in splitted_ids):
                        result.append(journal)
                        seen.add(journal)
                        break
            
        return result

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, area_ids: set[str], category_ids: set[str], quartiles: set[str]):
        result = []
        area_identifiers = set()
        cat_identifiers = set()

        for handler in self.categoryQuery:
            path = handler.dbPathOrUrl
            
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
                area_identifiers.update(df_area["identifier"].tolist())
                cat_identifiers.update(df_cat["identifier"].tolist())
       
        # Intersection
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