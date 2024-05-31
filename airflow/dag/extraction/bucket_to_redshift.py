import configparser
import pathlib
import psycopg2
import sys
from psycopg2 import sql
from datetime import datetime

"""
Part of DAG. Upload S3 CSV data to Redshift. Takes one argument of format YYYYMMDD. This is the name of 
the file to copy from S3. Script will load data into temporary table in Redshift, delete 
records with the same post ID from main table, then insert these from temp table (along with new data) 
to main table. This means that if we somehow pick up duplicate records in a new DAG run,
the record in Redshift will be updated to reflect any changes in that record, if any (e.g. higher score or more comments).
"""

# Parse our configuration file
script_path = pathlib.Path(__file__).parent.parent.parent.parent.resolve()
parser = configparser.ConfigParser()
parser.read(f"{script_path}/config.ini")

# Store our configuration variables
USERNAME = parser.get("aws_config", "redshift_username")
PASSWORD = parser.get("aws_config", "redshift_password")
HOST = parser.get("aws_config", "redshift_hostname")
PORT = parser.get("aws_config", "redshift_port")
REDSHIFT_ROLE = parser.get("aws_config", "redshift_role")
DATABASE = parser.get("aws_config", "redshift_database")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
ACCOUNT_ID = parser.get("aws_config", "account_id")
TABLE_NAME = "reddit"

output_name = datetime.now().strftime("%Y%m%d")


# Our S3 file & role_string
file_path = f"s3://{BUCKET_NAME}/{output_name}.csv"
role_string = f"arn:aws:iam::{ACCOUNT_ID}:role/{REDSHIFT_ROLE}"

# Create Redshift table if it doesn't exist
sql_create_table = sql.SQL(
    """CREATE TABLE IF NOT EXISTS {table} (
                            id varchar PRIMARY KEY,
                            title varchar(max),
                            score int,
                            edited bool,
                            num_comments int,
                            author varchar(max),
                            created_utc timestamp,
                            url varchar(max),
                            upvote_ratio float,
                            over_18 bool,
                            stickied bool
                        );"""
).format(table=sql.Identifier(TABLE_NAME))

# If ID already exists in table, we remove it and add new ID record during load.
create_temp_table = sql.SQL(
    "CREATE TEMP TABLE our_staging_table (LIKE {table});"
).format(table=sql.Identifier(TABLE_NAME))
sql_copy_to_temp = f"COPY our_staging_table FROM '{file_path}' iam_role '{role_string}' IGNOREHEADER 1 DELIMITER ',' CSV;"
delete_from_table = sql.SQL(
    "DELETE FROM {table} USING our_staging_table WHERE {table}.id = our_staging_table.id;"
).format(table=sql.Identifier(TABLE_NAME))
insert_into_table = sql.SQL(
    "INSERT INTO {table} SELECT * FROM our_staging_table;"
).format(table=sql.Identifier(TABLE_NAME))
drop_temp_table = "DROP TABLE our_staging_table;"


def main():
    """Upload file form S3 to Redshift Table"""
    rs_conn = connect_to_redshift()
    load_data_into_redshift(rs_conn)


def connect_to_redshift():
    """Connect to Redshift instance"""
    try:
        rs_conn = psycopg2.connect(
            dbname=DATABASE, user=USERNAME, password=PASSWORD, host=HOST, port=PORT
        )
        return rs_conn
    except Exception as e:
        print(f"Unable to connect to Redshift. Error {e}")
        sys.exit(1)


def load_data_into_redshift(rs_conn):
    """Load data from S3 into Redshift"""
    try:
        with rs_conn:

            cur = rs_conn.cursor()
            cur.execute(sql_create_table)
            cur.execute(create_temp_table)
            cur.execute(sql_copy_to_temp)
            cur.execute(delete_from_table)
            cur.execute(insert_into_table)
            cur.execute(drop_temp_table)

            # Commit only at the end, so we won't end up
            # with a temp table and deleted main table if something fails
            rs_conn.commit()
            print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data into Redshift: {e}")
        rs_conn.rollback()
        query_load_errors(cur)
    finally:
        cur.close()

# Function to query stl_load_errors
def query_load_errors(cur):
    """Query the stl_load_errors table to find the cause of the error"""
    error_query = """
        SELECT
            starttime,
            session,
            tbl,
            query,
            filename,
            line_number,
            colname,
            type,
            raw_field_value,
            err_code,
            err_reason
        FROM stl_load_errors;
    """
    cur.execute(error_query, (TABLE_NAME,))
    errors = cur.fetchall()
    print("Load errors:")
    for error in errors:
        print(error)

# Main function
def main():
    """Upload file from S3 to Redshift Table"""
    rs_conn = connect_to_redshift()
    load_data_into_redshift(rs_conn)


if __name__ == "__main__":
    rs_conn = connect_to_redshift()
    cur = rs_conn.cursor()
    cur.execute('SELECT * FROM reddit;')
    print(cur.fetchall())
    cur.close()