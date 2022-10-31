import typer

from parser.parser import parse_yaml_from_path
from databases.database_abstraction import start_migration


def main(
    path: str = typer.Option("./dbconfig.yaml", help="/the/path/to/your/dbconfig.yaml")
):
    data = parse_yaml_from_path(path)
    source = data.pop("source")
    destination = data.pop("destination")
    rules = data.pop("rules")
    try:
        start_migration(source, destination, rules)
    except Exception:
        print("Failed to get db_connection! Check DB name or ARGS")
    # [schema, source_data] = extract_from_source(**source)


if __name__ == "__main__":
    typer.run(main)
