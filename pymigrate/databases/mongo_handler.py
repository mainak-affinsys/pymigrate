import datetime

from pymongo import MongoClient
from sqlalchemy import Table


def get_headers(table: Table):
    return [str(col).split(".")[1] for col in table._columns]


def normalize(entity):
    if type(entity) == datetime.date:
        return datetime.datetime.combine(entity, datetime.datetime.min.time())
    return entity


def get_data_dict(data, headers):
    res = []
    for dat in data:
        res.append({headers[i]: normalize(dat[i]) for i, _ in enumerate(dat)})
    return res


def handle_mongo_insert(tables, data, client: MongoClient):
    for i, _ in enumerate(tables):
        headers = get_headers(tables[i])
        d = get_data_dict(data[i], headers)
        collection = client["cluster0"][tables[i].name]
        try:
            res = collection.insert_many(d)
        except Exception as e:
            print(e)
