
class IdentifiableEntity(object):
    def __init__(self, id):
        if isinstance(id, list):
            self.id = id
        elif isinstance(id, str):
            self.id = [id]
    
    def getIds(self):
        return self.id

class Journal(IdentifiableEntity):
    def __init__(self, id, title, languages, publisher, seal, licence, apc, hasCategory=None, hasArea=None):
        # the constructor of the superclass is explicitly recalled, so as to handle the input parameters as done in the superclass
        super().__init__(id)

        # defining the attributes
        self.title = title
        if isinstance(languages, str):
            self.languages = [languages]
        elif isinstance(languages, list):
            self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc

         # defining the relations
        if isinstance(hasCategory, str):
            self.hasCategory = [hasCategory]
        elif isinstance(hasCategory, list):
            self.hasCategory = hasCategory

        if isinstance(hasArea, str):
            self.hasArea = [hasArea]
        elif isinstance(hasArea, list):
            self.hasArea = hasArea
        
    # defining the methods of the class
    def getTitle(self):
        return self.title
    
    def getLanguages(self):
        return self.languages
    
    def getPublisher(self):
        return self.publisher
    
    def hasDOAJSeal(self):
        if self.seal == True:
            return True
        else:
            return False
    
    def getLicence(self):
        return self.licence
    
    def hasAPC(self):
        if self.apc == True:
            return True
        else:
            return False
    
    def getCategories(self):
        return self.hasCategory

    def getAreas(self):
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
