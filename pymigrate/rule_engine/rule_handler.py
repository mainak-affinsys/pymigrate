from sqlalchemy import select, Table

from pymigrate.rule_engine.ruleengine import RuleEngine


# Main function and the entry point to the file.
# Iterates over the tables, gets the query(with all rules applied) for each table and calls execution function
def get_data(engine, tables: list[Table], rules):
    # q = select(table.columns, func.count()).where(True).order_by(table.id)
    data = []
    try:
        for table in tables:
            if table.name not in rules:
                r = RuleEngine(table, {}, engine)
                data.append(r.exec_command(select))
                continue
            q = None
            ruleset = rules.pop(table.name)
            r = RuleEngine(table, ruleset, engine)
            try:
                q = r.get_base()
            except Exception as e:
                print("Error in get_base")
                print(e)
            try:
                data.append(r.exec_command(q))
            except Exception as e:
                print("in get_data \n", e)
    except Exception as e:
        print("in getdata", e)

    return data
