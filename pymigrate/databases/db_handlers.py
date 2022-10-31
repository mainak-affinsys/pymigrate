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
    db_name, user, password, host, port = data.get('db_name'), \
                                          data.get('credentials').get('user'), \
                                          data.get('credentials').get('pass'), \
                                          data.get('host'), data.get('port')
    if st.get(dbms) is not None:
        return f"{st[dbms]}{user}:{password}@{host}:{port}/{db_name}"
    raise Exception("Database not supported")


def handler(dbms, source, destination, rules):
    source_uri = source.get('connection_string',get_uri(dbms, source))
    destination_uri = destination.get('connection_string',get_uri(destination.get('dbms'), destination))
    source_engine = create_engine(source_uri)
    destination_engine = create_engine(destination_uri)
    meta = MetaData(bind=source_engine)
    meta.reflect()
    tables: list[Table] = [meta.tables[table] for table in meta.tables]

    data = get_data(source_engine, tables, rules)
    # [source_engine.execute(select(table)).all() for table in tables]

    meta.create_all(destination_engine)
    for i, _ in enumerate(tables):
        [destination_engine.execute(tables[i].insert().values(d)) for d in data[i]]


db = {
    ("postgres", "pg", "postgre", "postgresql"): "postgresql",
    ("mysql", "sql"): "mysql",
    ("sqllite", "sqlite"): "sqlite",
}
