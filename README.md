# PyMigrate

A simple ETL script written in python to migrate data from one database to another.

## Basic Usage
The script can be installed using the `binary` in the **RELEASES** section.

Call the `binary` from the shell using the command

```shell
>> pymigrate --path /path/to/your/dbconfig.yaml
```

## dbconfig.yaml

The `dbconfig.yaml` file is the main source file of our script. The 
parameters set defines the behaviour of our script.

The basic structure of the yaml file is as follows:
```yaml
source: # The source db from where we are getting the data from
  dbms: "postgres" # The DBMS used for the db, will define the Extraction behaviour
  db_name: "pg" # Name of the database
  credentials: # Login credentials for the database
    user: "author" # Username for database login
    pass: "password" # Password for connection
  host: "localhost" # Host for connection, `localhost` if hosted locally
  port: "5432" # Port where the server is hosted at
  connection_string: "*" # An optional field which can contain the connection string following the sqlalchemy url format(https://docs.sqlalchemy.org/en/20/core/engines.html#database-urls)

destination: # The destination db where we are going to save our data
  dbms: "mysql"
  db_name: "affinsys_bankbuddy"
  credentials:
    user: "author"
    pass: "mysql"
  host: "localhost"
  port: "3306"
```
``` yaml
rules: # (OPTIONAL) The transformation rules for the data, can be defined to get only certain columns, data, or querying
  table_name: # Select the table to apply the rule to
    # Optional param if we want to handle a particular column values,
    # Example: Only get Accounts[TABLE] which have last_accessed[COLUMN] within 3years[QUERY]
```

We have 3 options with the Where clause
- either use a singular query with multiple clauses joined by and/or, like
```yaml
    where: # Select type of rule
      query: "name='something' and id=2 or joining_date>'2011-10-14'"

```
- or a Single structured clause, like 
```yaml
    where:
      column: "contract_status"
      operator: "="
      value: "True"
      
```
- Or we can use a And or Structured clause with mix of both
```yaml
where:
  or:
    - query: "customer.contract_status = 'False'"

    - and:
        - query: "customer.name like '%in%'"

        #        - column: "contract_status"
        #          operator: "="
        #          value: "True"
        - column: "date"
          operator: ">"
          value: "2022-10-21"
```

## Working

### Packages Used
 - `poetry` [ `pip` replacement. For package management and development environment ]
 - `ruamel.yaml` [ To parse yaml files ]
 - `typer` [ For CLI creation and handling command line arguments and commands ]
 - `sqlalchemy` [ For handling the database abstraction and multiple database homogeneity ]
 - `pymysql` [ As the default Connector for mysql ]
 - `psycopg2-binary` [ As the default Connector for postgresql ]

### Understanding code

<h4>Directory Structure:<h4>
```bash
├── LICENSE
├── poetry.lock
├── .gitignore
├── pymigrate
│   ├── databases
│   │   ├── database_abstraction.py
│   │   ├── db_handlers.py
│   │   └── __init__.py
│   ├── __init__.py
│   ├── main.py
│   ├── parser
│   │   ├── __init__.py
│   │   └── parser.py
│   ├── rule_engine
│   │   ├── __init__.py
│   │   ├── ruleengine.py
│   │   └── rule_handler.py
│   └── utils
│       ├── __init__.py
│       └── utils.py
├── pyproject.toml
└── README.md

7 directories, 25 files
```

<h4>Working</h4>
The File main.py acts as the entry point. The commands and CLI is processed
using the `Typer` library. The processed path is then passed to the `parser.parser` file function to
be processed into a `JSON` obj or a python `dict` object.

The JSON is then divided into 3 further, smaller, `dict`s which are the 

- source [DB] 
- destination [DB]
- rules

The three are then passed to the db_handler which handles fetching of the table
schemas from the source, creating them at the destination and also inserting fetched
data from the source.

Speaking of fetching the data from the source, that's where the rules are applied.
Users can specify rules which can manipulate and filter out data which we want to
enter into the database. The rules are then parsed to form a proper select query
which is then executed and returned from the rule_handler.

### Contribution

<h4>Running on Local engine</h4>
The environment as well as the engine used to run and develop the 
project is poetry. Thus, we need poetry as one of the primary dependencies.

- Clone the repository and cd into directory
```shell
>> git clone https://github.com/mainak-affinsys/pymigrate.git pymigrate
>> cd pymigrate
```
- Install poetry
```shell
>> pip install poetry
```
- Init environment and install all dependencies
```shell
>> poetry env use $(which python3)
>> poetry install
```
- Run the script using the first command.
