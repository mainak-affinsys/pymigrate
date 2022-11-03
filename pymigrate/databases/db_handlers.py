from sqlalchemy import create_engine, MetaData, Table
from pymongo import MongoClient


from pymigrate.rule_engine.rule_handler import get_data
from pymigrate.databases.mongo_handler import handle_mongo_insert
from pymigrate.rule_engine.mongo_ruleengine import getData

st = {
    "postgresql": "postgresql://",
    "mysql": "mysql+pymysql://",
    "oracle": "oracle://",
    "mssql": "mssql+pydoc://",
    "sqlite": "sqlite://",
}


def get_uri(dbms, data):
    db_name, user, password, host, port = (
        data.get("db_name"),
        data.get("credentials").get("user"),
        data.get("credentials").get("pass"),
        data.get("host"),
        data.get("port"),
    )
    if st.get(dbms) is not None:
        return f"{st[dbms]}{user}:{password}@{host}:{port}/{db_name}"
    raise Exception("Database not supported")


def select_modifier(rules, t, type_sel):
    sel = rules.get(t.name).pop(type_sel)
    sel = sel.pop("columns")
    for i in t._columns:
        i = str(i).split(".")[1]
        if type_sel == "select":
            if i not in sel:
                t._columns.remove(t._columns[i])
        elif type_sel == "no_select":
            if i in sel:
                t._columns.remove(t._columns[i])
    return t._columns


def get_create_engine_handler(dbms):
    if dbms == "mongodb":
        return MongoClient
    for key in db.keys():
        if dbms in key:
            return create_engine


def get_sqldb_data(source_engine, rules):
    meta = MetaData(bind=source_engine)
    meta.reflect()

    for table in meta.tables:
        t = meta.tables[table]
        try:
            if t.name in rules and "select" in rules.get(t.name):
                t._columns = select_modifier(rules, t, "select")
            if t.name in rules and "no_select" in rules.get(t.name):
                t._columns = select_modifier(rules, t, "no_select")
        except Exception as e:
            print(e)
    tables = [meta.tables[table] for table in meta.tables]
    return (meta,tables,get_data(source_engine, tables, rules))


def handler(dbms, source, destination, rules):
    source_uri = (
        source.get("connection_string")
        if "connection_string" in source
        else get_uri(dbms, source)
    )
    destination_uri = (
        destination.get("connection_string")
        if "connection_string" in destination
        else get_uri(destination.get("dbms"), destination)
    )
    ce = get_create_engine_handler(dbms)
    source_engine = ce(source_uri)
    try:
        ce = get_create_engine_handler(destination.get("dbms"))
    except Exception as e:
        print(e)
    destination_engine = ce(destination_uri)
    if dbms != "mongodb":
        meta, tables, data = get_sqldb_data(source_engine, rules)

    else:
        try:
            data = getData(source_engine, rules)
            print(data)
        except Exception as e:
            print(e)

    # [source_engine.execute(select(table)).all() for table in tables]

    if destination.get("dbms") == "mongodb":
        handle_mongo_insert(tables, data, destination_engine)
        return

    try:
        meta.create_all(destination_engine)
    except Exception as e:
        print("IN create")
        print(e)
    for i, _ in enumerate(tables):
        [destination_engine.execute(tables[i].insert().values(d)) for d in data[i]]


db = {
    ("postgres", "pg", "postgre", "postgresql"): "postgresql",
    ("mysql", "sql"): "mysql",
    ("sqllite", "sqlite"): "sqlite",
    ("mongodb", "mongo"): "mongodb",
}
