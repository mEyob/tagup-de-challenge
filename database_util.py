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


def create_tables(connection):
    """Create tables specified in the 'schema' dict.

    Args:
        connection: database connection

    Returns:
        response: a dict of execution status and possibly
        errors
    """
    response = {}
    cursor = connection.cursor
    cursor = connection.cursor()

    for table in schema.keys():
        statement = f"CREATE TABLE IF NOT EXISTS {table}("
        for column, dtype in schema[table].items():
            statement += column + ' ' + dtype + ', '
        statement = statement.rstrip(', ')
        statement += ");"
        try:
            cursor.execute(statement)
            response["status"] = 0
        except psycopg2.DatabaseError as err:
            response["status"] = 1
            response["error"] = str(err)
    cursor.close()
    return response


def drop_table(connection, table):
    """Drop database table.

    Args:
        connection: database connection
        table: table to be dropped

    Returns:
        response: a dict of execution status and possibly
        errors
    """
    response = {}
    statement = f"DROP TABLE IF EXISTS {table}"
    cursor = connection.cursor()
    try:
        cursor.execute(statement)
        response["status"] = 0
    except psycopg2.DatabaseError as err:
        response["status"] = 1
        response["error"] = str(err)
    finally:
        cursor.close()
    return response


def create_hypertable(connection, table, column):
    """Create a timescaledb hypertable

    Args:
        connection: database connection
        table: table for which hypertable is going
        to be created
        column: the column used to create hypertable
        chunks

    Returns:
        response: a dict of execution status and possibly
        errors
    """
    response = {}
    cursor = connection.cursor()
    statement = f"SELECT create_hypertable('{table}', '{column}');"
    try:
        cursor.execute(statement)
        response["status"] = 0
    except psycopg2.DatabaseError as err:
        response["status"] = 1
        response["error"] = str(err)
    finally:
        cursor.close()
    return response


def setup_database():
    machine_count = 20
    con = connect()
    print(f"Connection request response: {con}")
    if con["status"] == 0:
        connection = con["con"]
        drop_table(connection, "example_co.measurements")
        drop_table(connection, "example_co.machines")

        resp = create_tables(connection)
        print(f"Table creation response: {resp}")

        resp = create_hypertable(connection, "example_co.measurements", "time")
        print(f"Hypertable creation response: {resp}")

        resp = insert(con["con"], "example_co.machines",
                      ("id", "name", "description"),
                      [(m_id, None, None)
                       for m_id in range(1, machine_count + 1)], True)
        print(f"Insert statement response: {resp}")


if __name__ == "__main__":
    setup_database()
