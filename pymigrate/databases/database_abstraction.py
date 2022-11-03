from .db_handlers import handler, db


def start_migration(source, destination, rules):
    try:
        for keys in db.keys():
            if source.get("dbms") in keys:
                return handler(db[keys], source, destination, rules)
    except Exception as e:
        print(e)
