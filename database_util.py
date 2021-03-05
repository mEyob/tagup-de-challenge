import os
import json
import psycopg2
from psycopg2.extras import execute_values

schema = {
    "example_co.machines": {
        "id": "SMALLINT PRIMARY KEY",
        "name": "VARCHAR(25)",
        "description": "TEXT"
    },
    "example_co.measurements": {
        "time": "TIMESTAMP NOT NULL",
        "machine_id": "SMALLINT REFERENCES example_co.machines (id)",
        "metric1": "NUMERIC(6,2)",
        "metric2": "NUMERIC(6,2)",
        "metric3": "NUMERIC(6,2)",
        "metric4": "NUMERIC(6,2)",
    }
}


def connect():
    """Create connection to the example_co database

    Returns:
        A dict containing a psycopg2 connection object OR 
        an error message
    """
    response = {}
    con_args = json.loads(os.environ["DBCON"])
    try:
        connection = psycopg2.connect(**con_args)
        response["status"] = 0
        response["con"] = connection
    except psycopg2.DatabaseError as err:
        response["status"] = 1
        response["error"] = str(err)
    return response


def insert(connection, table, columns, records, batch=False):
    """Batch insert rows into example_co database.

    Args:
        connection: database connection
        table: the db table where new 'records' will be inserted
        columns: corresponding columns that will get new records 
        records: values to be plugged into the `VALUES` 
        part of the insert statement
        batch (boolean): when true, records are inserted in batches

    Returns:
        response: dict of execution status and possible erros
    """

    response = {}
    statement = f"INSERT INTO {table} ({','.join(columns)})  VALUES %s;"
    cursor = connection.cursor()
    try:
        if batch:
            execute_values(cursor, statement, records)
        else:
            cursor.execute(statement, records)
        connection.commit()
        response["status"] = 0
    except psycopg2.DatabaseError as err:
        connection.rollback()
        response["status"] = 1
        response["error"] = str(err)
    finally:
        cursor.close()
    return response
