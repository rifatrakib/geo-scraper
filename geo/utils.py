from geo import settings


class MongoConnectionManager:
    def __init__(self, collection):
        self.client = settings.MONGO_URI
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
