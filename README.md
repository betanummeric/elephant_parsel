# elephant_parsel
 wrapper for PostgreSQL queries with transaction and connection pooling support 

This project is about making [psycopg2](https://www.psycopg.org/) easier to use.

## supported versions
Debian >=10, psycopg2 >=2.7, python >=3.7

## installation

install using [pipenv](https://pipenv.pypa.io/en/latest/):
```bash
pipenv install elephant_parsel
```

## basic usage

```python3
from elephant_parsel.postgres_db import PostgresDB, PostgresDBException
import logging
db = PostgresDB(connection_config=dict(
    host='db.example.com',
    port=5432,
    dbname='the_database_name',
    user='the_username',
    password='****',
    minconn=1,
    maxconn=4
  ), logger=logging.getLogger(), connect=True, register_hstore=False)

server_version_string = db.query_one('select version()', None, column='version')
```
The `connection_config` is used as kwargs for the `psycopg2.ThreadedConnectionPool`, so it should support all libpq connection variables: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS


