from pymongo import MongoClient
from mongodb.config import DB_NAME, DB_URL, LOGGER

client = MongoClient(DB_URL)
db_cursor = client[DB_NAME]
table = db_cursor.logs.find({})

log_list = []

for document in table:
    del document['_id']
    log_list.append(document)
    if len(log_list) > 100:
        log_list = []



print(log_list)
