from .db_handlers import handler, db


def start_migration(source, destination, rules):
    for keys in db.keys():
        if source.get("dbms") in keys:
            return handler(db[keys], source, destination, rules)
    raise Exception("Incorrect DB!!")
