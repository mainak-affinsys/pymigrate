from pymongo import MongoClient
from pymongo.database import Database


def get_wrapper(d):
    if "and" in d:
        return "$and", d.pop("and")
    if "or" in d:
        return "$or", d.pop("or")
    return None, d


def p_op(operator):
    mapping = {
        "=" : "$eq",
        "!=" : "$neq",
        ">": "$gt",
        "<": "$lt",
        "like": "$regex",
        ">=": "$gte",
        "<=": "$lte",
    }

    return mapping[operator]


def get_regex(val):
    val = [i for i in val if i != "'"]
    val[0] = "/^"+val[0] if val[0] != "%" else "/"

    val[-1] = val[-1]+"$/" if val[-1] != "%" else "/"
    return "".join(val)


def parse_command(comm):
    if comm.get('query') is None:
        col, op, value = (
            comm.get("column"),
            comm.get("operator"),
            comm.get("value"),
        )
    else:
        col, op, value = comm.get('query').split(" ")
        col = col.split(".")[1]
        if value in ["false", "False", "'False'", "'false'"]:
            value = False
        elif value in ["true", "True", "'true'", "'True'"]:
            value = True

        if op == "like":
            value = get_regex(value)

    q = {col: {p_op(op): value}}

    return q


def iterate_where(rule_set):
    res = []
    l = {}
    for rule in rule_set:
        if "and" in rule or "or" in rule:
            wrp, r = get_wrapper(rule)
            l[wrp] = iterate_where(r)
            res.append(l)
            continue

        res.append(parse_command(rule))
    return res


def get_clauses(where, select, no_select):
    sel = {"_id": 0}
    if select is not None:
        for col in select.get('columns'):
            sel[col] = 1
    elif no_select is not None:
        for col in no_select.get('columns'):
            sel[col] = 0

    if "and" in where or "or" in where:
        wrp, r = get_wrapper(where)
        where_clause = {}
        where_clause[wrp] = iterate_where(r)
    else:
        where_clause = parse_command(where)
    return (sel, where_clause)


def getData(client: MongoClient, rules):
    try:
        db:Database = client.cluster0

        data = []

        curr = db.list_collections()
        tables = []
        while curr.alive:
            tables.append(curr.next())

        for table in tables:
            t = []
            if table.get('name') in rules:
                rule = rules.pop(table.get('name'))
                where = rule.pop('where',None)
                select = rule.pop('select',None)
                no_select = rule.pop('no_select',None)
                sel, where_cl = get_clauses(where, select, no_select)
                curr = db[table.get('name')].find(where_cl, sel)
            else:
                curr = db[table.get('name')].find({},{"_id":0})
            while curr.alive:
                t.append(curr.next())
            data.append(t)

        return data


    except Exception as e:
        print(e)

    pass