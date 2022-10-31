from sqlalchemy import create_engine, MetaData, Table

from pymigrate.rule_engine.rule_handler import get_data

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


def handler(dbms, source, destination, rules):
    source_uri = source.get("connection_string", get_uri(dbms, source))
    destination_uri = destination.get(
        "connection_string", get_uri(destination.get("dbms"), destination)
    )
    source_engine = create_engine(source_uri)
    destination_engine = create_engine(destination_uri)
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
    tables: list[Table] = [meta.tables[table] for table in meta.tables]

    data = get_data(source_engine, tables, rules)
    # [source_engine.execute(select(table)).all() for table in tables]

    try:
        meta.create_all(destination_engine)
    except Exception as e:
        print(e)
    for i, _ in enumerate(tables):
        [destination_engine.execute(tables[i].insert().values(d)) for d in data[i]]


db = {
    ("postgres", "pg", "postgre", "postgresql"): "postgresql",
    ("mysql", "sql"): "mysql",
    ("sqllite", "sqlite"): "sqlite",
}
