import os
import logging
import numpy as np
import pandas as pd
import database_util

STAGING_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "staging")
LOG_PATH = os.path.join(os.path.dirname(__file__), "log")
COLUMNS = ("time", "metric1", "metric2", "metric3", "metric4")

logging.basicConfig(level=logging.INFO,
                    filename=os.path.join(LOG_PATH, "log"),
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt='%y-%m-%d %H:%M:%S')


def csv_to_database(csv_file, col_names=COLUMNS):
    try:
        machine_id = csv_file.lstrip("machine_").rstrip(".csv")
        machine_id = int(machine_id)

    except:
        return {
            "status": 1,
            "error": "Cannot extract machine id from the filename"
        }
    csv_file_path = os.path.join(STAGING_PATH, csv_file)
    df = pd.read_csv(csv_file_path, header=0, names=col_names)
    records = [tuple(row) for row in df.values]
    records = np.insert(records, 1, machine_id, axis=1)
    columns = np.insert(df.columns.values, 1, "machine_id")

    connection = database_util.connect()["con"]
    response = database_util.insert(connection,
                                    "example_co.measurements",
                                    columns,
                                    records,
                                    batch=True)
    if response.get("status") == 0:
        os.remove(csv_file_path)
        return df.describe().apply(round, ndigits=2)
    else:
        logging.error(response.get("error"))
        return response
