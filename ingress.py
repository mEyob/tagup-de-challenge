import os
import logging
import numpy as np
import pandas as pd
import database_util
import gdrive_download

STAGING_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                            "staging")
LOG_PATH = os.path.join(os.path.dirname(__file__), "log")
COLUMNS = ("time", "metric1", "metric2", "metric3", "metric4")
WRONG_READING_CUTOFF = 100

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
    rows, df = filter_dataframe(df)

    if rows > 0:
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
            return {
                "status": 0,
                "summary_stat": df.describe().apply(round, ndigits=2)
            }
        else:
            logging.error(response.get("error"))
            return response
    else:
        return {"status": 1, "error": "All entries contain erroneous value"}


def filter_dataframe(df, cutoff=WRONG_READING_CUTOFF):
    """filter out faulty readings from a pandas dataframe.

    Args:
        df: the pandas dataframe to be filtered
        cutoff: threshold between normal & faulty readings.
        Defaults to FAULTY_READING_CUTOFF.

    Returns:
        df: a filtered pandas dataframe
    """
    metric_cols = df.columns[1:]
    df[metric_cols] = df[metric_cols].applymap(lambda x: x
                                               if abs(x) < cutoff else None)
    rows = df[metric_cols].dropna(how='all').shape[0]
    return rows, df


def main():
    """Download new measurements from Google Drive and 
    write them to database.
    """
    gdrive_download.main()
    for csv_file in os.listdir(STAGING_PATH):
        csv_to_database(csv_file)


if __name__ == "__main__":
    main()
