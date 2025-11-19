# Supposing that all the classes developed for the project
# are contained in the file 'impl.py', then:

# 1) Importing all the classes for handling the relational database
from impl import CategoryUploadHandler, CategoryQueryHandler

# 2) Importing all the classes for handling graph database
from impl import JournalUploadHandler, JournalQueryHandler

# 3) Importing the class for dealing with mashup queries
from impl import FullQueryEngine

# Once all the classes are imported, first create the relational
# database using the related source data
rel_path = "relational.db"
cat = CategoryUploadHandler()
cat.setDbPathOrUrl(rel_path)
# cat.pushDataToDb("data/scimago.json")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# Then, create the graph database (remember first to run the
# Blazegraph instance) using the related source data
grp_endpoint = "http://127.0.0.1:9999/blazegraph/sparql"
jou = JournalUploadHandler()
jou.setDbPathOrUrl(grp_endpoint)
# jou.pushDataToDb("data/doaj.csv")
# Please remember that one could, in principle, push one or more files
# calling the method one or more times (even calling the method twice
# specifying the same file!)

# In the next passage, create the query handlers for both
# the databases, using the related classes
cat_qh = CategoryQueryHandler()
cat_qh.setDbPathOrUrl(rel_path)

jou_qh = JournalQueryHandler()
jou_qh.setDbPathOrUrl(grp_endpoint)

# Finally, create a advanced mashup object for asking
# about data
que = FullQueryEngine()
que.addCategoryHandler(cat_qh)
que.addJournalHandler(jou_qh)

# result_q1 = len(que.getAllJournals())
# result_q2 = que.getJournalsInCategoriesWithQuartile({"Artificial Intelligence", "Oncology"}, {"Q1"})
# result_q3 = que.getEntityById("Artificial Intelligence")
# result_q4 = que.getEntityById("2532-8816")
result = que.getDiamondJournalsInAreasAndCategoriesWithQuartile({"Arts and Humanities"}, {"Arts and Humanities (miscellaneous)"}, {"Q1", "Q2"})
for i in result:
    print(i.getTitle())

print(len(result))


import time
from datetime import datetime

def time_long_run(func, *args, **kwargs):
    print(f"Starting '{func.__name__}' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    start_wall = time.time()
    start_perf = time.perf_counter()

    result = func(*args, **kwargs)

    end_wall = time.time()
    end_perf = time.perf_counter()

    print(f"Finished '{func.__name__}' at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total time (wall clock): {(end_wall - start_wall)/60:.2f} minutes")
    print(f"Total time (CPU/perf counter): {(end_perf - start_perf):.2f} seconds")

    return result


# --- Usage Example ---
# Instead of calling your function like this:
#    output = your_method(x, y, z)

# Do this:
# output = time_long_run(que.getAllJournals())
# print(output)
# etc...










