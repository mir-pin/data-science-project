from sqlite3 import connect
from pandas import read_sql

class IdentifiableEntity(object):
    def __init__(self, id):
        # option 1
        # self.id = set()
        # for identifier in identifiers.split(","):
        #     self.id.add(identifier.strip())

        # option 2
        if isinstance(id, str):
            self.id = id
        elif isinstance(id, list):
            self.id = ", ".join(id)
    

    def getIds(self):
        return self.id
    
class Journal(IdentifiableEntity):
    def __init__(self, id, title, languages, publisher, seal, licence, apc, hasCategory=None, hasArea=None):

        # the constructor of the superclass is explicitly recalled, so as
        # to handle the input parameters as done in the superclass
        super().__init__(id)

        # defining the attributes
        self.title = title
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc
         # defining the relations
        self.hasCategory = hasCategory
        self.hasArea = hasArea

        if isinstance(languages, str):
            self.languages = languages
        elif isinstance(languages, list):
            self.languages = ", ".join(languages)

    # defining the methods of the class
    def getTitle(self):
        return self.title
    
    def getLanguages(self):
        return self.languages
    
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
        
        # option 1
        #string_id = ", ".join(self.id)
        
        # if not self.hasCategory:
        #     with connect("rel.db") as con:
                
        #         query = f"""
        #             SELECT DISTINCT category_name
        #             FROM JournalCategories
        #             LEFT JOIN Categories ON JournalCategories.category_id = Categories.category_id
        #             LEFT JOIN JournalIds ON JournalIds.journal_id = JournalCategories.journal_id
        #             WHERE identifier = ("{string_id}");
        #             """
                    
        #         df = read_sql(query, con)

        #     for _, row in df.iterrows():
        #         item = (Category(id=row["category_name"]))
        #         result.append(item.id)

        #     return result

        # option 2
        if not self.hasCategory:
            with connect("rel.db") as con:
                query = """
                    SELECT DISTINCT category_name
                    FROM JournalCategories
                    LEFT JOIN Categories ON JournalCategories.category_id = Categories.category_id
                    LEFT JOIN JournalIds ON JournalIds.journal_id = JournalCategories.journal_id
                    WHERE identifier = ? ;
                    """
                    
                df = read_sql(query, con, params=(self.id,))

            for _, row in df.iterrows():
                item = (Category(id=row["category_name"]))
                result.append(item.id)

            return result
    
        else:
            return self.hasCategory
        
    def getAreas(self):
        result = []
        string_id = ", ".join(self.id)
        
        if not self.hasArea:
            with connect("rel.db") as con:
                
                query = f"""
                    SELECT DISTINCT area_name
                    FROM JournalAreas
                    LEFT JOIN Areas ON JournalAreas.area_id = Areas.area_id
                    LEFT JOIN JournalIds ON JournalIds.journal_id = JournalAreas.journal_id
                    WHERE identifier = ("{string_id}");
                    """
                    
                df = read_sql(query, con)

            for _, row in df.iterrows():
                item = (Area(id=row["area_name"]))
                result.append(item.id)

            return result
        else:
            return self.hasArea


class Category(IdentifiableEntity):
    def __init__(self, id, quartile=None):
        super().__init__(id)
        self.quartile = quartile
        
    
    # defining the method of the class Category
    def getQuartile(self):
        return self.quartile
    

class Area(IdentifiableEntity):
    def __init__(self, id):
        super().__init__(id)


my_file = Journal("2237-9363", "Journal of Coloproctology","English","Thieme Revinter Publicações Ltda.", "No", "CC BY, CC BY-NC-ND", "No")
our_file = Journal("0514-7336", "Zephyrus", ["English, Spanish"], "Ediciones Universidad de Salamanca", "No", "CC BY-NC-ND", "No")

prova = our_file.getCategories()
prova_2 = my_file.getCategories()
# prova_2 = our_file.getAreas()

# print(prova_2)
print(prova)
# print(prova)
