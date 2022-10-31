from sqlalchemy import and_, or_, text, select, func
from pymigrate.utils.utils import gfk


class RuleEngine:
    def __init__(self, table, rules, engine):
        self.table = table
        self.rules = rules
        self.engine = engine

    # Gets the wrapper for where clause from the `and_` and the `or_`
    @staticmethod
    def get_wrapper(d):
        if "and" in d:
            return and_, d.pop('and')
        if "or" in d:
            return or_, d.pop('or')
        return None, d

    # Parses the parameterized yaml data and returns a string command/query
    def parse_command(self, comm):
        col, op, value = ".".join([self.table.name, comm.get('column')]), \
                         comm.get('operator'), comm.get('value')

        return f"{col} {op} '{value}'"

    # Iterative function which helps generate the clauses from nested AND/OR statements/clauses
    def iterate_where(self, rule_set):
        l = []
        for rule in rule_set:
            if 'and' in rule or 'or' in rule:
                wrp, r = self.get_wrapper(rule)
                ans = self.iterate_where(r)
                l.append(wrp(*ans))
                continue

            if 'query' in rule:
                try:
                    l.append(text(rule.get('query')))
                except Exception as e:
                    print(e)
                continue
            l.append(text(self.parse_command(rule)))
        return l

    # Main generator function for the where clauses
    def generate_where_clause(self):
        wrp, self.rules = self.get_wrapper(self.rules.get('where'))

        if wrp is None:
            return self.parse_command(text(self.rules[0]))

        l = self.iterate_where(self.rules)

        return wrp(*l)

    # Gets the base query for execution, parses the ruleset and returns a query function which can be called
    # with table object as argument
    def get_base(self):

        def where(table):
            clauses = None
            try:
                clauses = self.generate_where_clause()
            except Exception as e:
                print("Error in where clause")
                print(e)
            return select(*table.columns).where(clauses)

        def count_group(table):
            self.rules = self.rules.get('count')
            col = self.rules.get('column')
            pk = self.rules.get('primary_key', 'id')
            label = self.rules.get('label', 'count')
            return select(table.c[col], func.count(table.c[pk]).label(label)).group_by(table.c[col])

        function_rule_set = {
            "where": where,
            "count": count_group
        }

        return function_rule_set[gfk(self.rules)]

    # Executes the table query for specified DB engine
    def exec_command(self, query):
        try:
            dat = self.engine.execute(query(self.table)).all()
            return dat
        except Exception as e:
            print(e)
    pass
