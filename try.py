import json
from json import load
import pprint
import pandas as pd
from pandas import read_sql, Series, DataFrame, merge
import sqlite3 
from sqlite3 import connect   

from impl import CategoryUploadHandler, CategoryQueryHandler

rel_path = "rel.db"
cat = CategoryUploadHandler()
cat.setDbPathOrUrl(rel_path)
cat.pushDataToDb("scimago_last_ten.json")

cat_qh = CategoryQueryHandler()
cat_qh.setDbPathOrUrl("rel.db")

result_1 = cat_qh.getCategoriesWithQuartile({})
# pprint.pp(result_1)

result_2 = cat_qh.getCategoriesAssignedToAreas({'Social Sciences'})
# pprint.pp(result_2)

result_3 = cat_qh.getAreasAssignedToCategories({'Gender Studies', 'Urology'})
# pprint.pp(result_3)

# getbyId
result_4 = cat_qh.getById("Gender Studies")
# pprint.pp(result_4)









