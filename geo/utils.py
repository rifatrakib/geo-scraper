import json

from pymongo import MongoClient

from geo import settings


class MongoConnectionManager:
    def __init__(self, collection):
        self.client = MongoClient(settings.MONGO_URI)
        self.database = settings.MONGO_DATABASE
        self.collection = collection

    def __enter__(self):
        self.database = self.client[self.database]
        self.collection = self.database[self.collection]
        return self.collection

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.client.close()


def extract_data_from_stored_records(name, field):
    projection = {"_id": 0, field: 1}
    with MongoConnectionManager(name) as session:
        data = list(session.find({}, projection))
    return data


def read_spider_tracker():
    with open("spider-tracker.json") as reader:
        records = json.loads(reader.read())
    return records


def read_last_record():
    with open("last-record.json") as reader:
        last_record = json.loads(reader.read())
    return last_record


def rollback_last_record_data(name, field, last_id):
    with MongoConnectionManager(name) as session:
        session.delete_many({field: last_id})


def prepare_scraping_session(name, field):
    history = read_last_record()
    last_id = history[field]
    rollback_last_record_data(name, field, last_id)

    last_index = history["index"] - 1
    data = read_spider_tracker()
    return data[last_index:]
