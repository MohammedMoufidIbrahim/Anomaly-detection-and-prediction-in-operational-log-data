from typing import Dict, List
from datetime import datetime

from pymongo import MongoClient
from mongodb.config import DB_NAME, DB_URL, LOGGER


class Factory:
    def __init__(self):
        self.cursor = self._get_cursor()

    @staticmethod
    def _get_cursor():
        """Access database
        Note:
            if database does not exist it will be automatically created
        """

        try:
            client = MongoClient(DB_URL)
            db_cursor = client[DB_NAME]
            LOGGER.info(f'accessed to {DB_NAME} database')
            return db_cursor
        except Exception as e:
            raise e

    def _get_collection(self, collection_name: str):
        """Access particular database collection
        Note:
            if collection does not exist it will be automatically created
        """

        try:
            db_collection = self.cursor[collection_name]
            LOGGER.info(f'created to {collection_name} collection')
            return db_collection
        except Exception as e:
            raise e

    def _insert_row(self, collection, data: Dict):
        LOGGER.info(f'inserted {len(data)} rows to {collection}')
        collection.insert_one(data)
        return True

    def _insert_rows(self, collection, data: List[Dict]):
        collection.insert_many(data)
        LOGGER.info(f'inserted {len(data)} rows to {collection}')
        return True

    def run(self, data):
        #collection = self._get_collection(collection_name=f'logs_{datetime.now().strftime("%H%M%S_%d%m%Y")}')  # collection name
        collection = self._get_collection(collection_name=f'logs')  # collection name
        # row = {"name": "John", "address": "Highway 37"}
        # rows = [
        #     {"name": "John", "address": "Highway 37"},
        #     {"name": "Alex", "address": "Lowdice 80"}
        # ]

        if len(data) != 1:
            if self._insert_rows(collection, data=data) is not True: '[Write Row] -- unexpected behavior for multiple rows'
        else:
            if self._insert_row(collection, data=data) is not True: '[Write Row] -- unexpected behavior for single row'

        return collection


if __name__ == "__main__":
    job = Factory().run()
    print(job)

