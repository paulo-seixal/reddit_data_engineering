import boto3
import botocore
import configparser
import pathlib
from datetime import datetime

"""
Part of DAG. Take Reddit data and upload to S3 bucket. Takes one command line argument of format YYYYMMDD. 
This represents the file downloaded from Reddit, which will be in the /tmp folder.
"""

# Load AWS credentials
parser = configparser.ConfigParser()
script_path = pathlib.Path(__file__).parent.resolve()
datasets_path = pathlib.Path(__file__).parent.parent.resolve() / "datasets"
parser.read(f"{script_path}/config.ini")
BUCKET_NAME = parser.get("aws_config", "bucket_name")
AWS_REGION = parser.get("aws_config", "aws_region")

output_name = datetime.now().strftime("%Y%m%d")


# Name for our S3 file
FILENAME = f"{output_name}.csv"
KEY = output_name + ".csv"


def main():
    """Upload input file to S3 bucket"""
    conn = connect_to_s3()
    create_bucket_if_not_exists(conn)
    upload_file_to_s3(conn)


def connect_to_s3():
    """Connect to S3 Instance"""
    try:
        conn = boto3.resource("s3")
        return conn
    except Exception as e:
        print(f"Can't connect to S3. Error: {e}")



def create_bucket_if_not_exists(conn):
    """Check if bucket exists and create if not"""
    exists = True
    try:
        conn.meta.client.head_bucket(Bucket=BUCKET_NAME)
    except botocore.exceptions.ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            exists = False
    if not exists:
        conn.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={"LocationConstraint": AWS_REGION},
        )


def upload_file_to_s3(conn):
    """Upload file to S3 Bucket"""
    conn.meta.client.upload_file(
        Filename= datasets_path / FILENAME, Bucket=BUCKET_NAME, Key=KEY
    )


if __name__ == "__main__":
    main()