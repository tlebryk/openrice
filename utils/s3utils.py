import boto3
import logging
from botocore.exceptions import ClientError
import re
import pandas as pd
import io

DEV = boto3.session.Session(profile_name="user1")


def upload_s3(file_name, object_name=None, s3_client=None, bucket="openrice"):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to. Default is protocolchina
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    # instantiate the client if not already specified
    if s3_client is None:
        s3_client = DEV.client("s3")
    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return(False)
    logging.info(f"Uploaded {file_name} to {object_name}")
    return(True)


def read_key_dflist(filedir, client=None, filepattern=None, bucket="openrice", quiet=False, min_page = None):
    """
    Reads in .csv files from S3 and returns list of (key, df) tuples
    :filedir: prefix of a boto3 search into s3
    :returns: list of (key, df) tuples where key is s3 path
        and df is a pandas dataframe
    """

    # instantiate S3 client if needed
    if client is None:
        client = DEV.client("s3")

    # get everything in the bucket and directory
    # need to create paginator, otherwise limits to 1000 objects
    logging.info(f"Reading from {filedir}")
    paginator = client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket, Prefix=filedir)
    is_match = True
    outlist = []
    for page in pages:
        for obj in page['Contents']:
            key = obj.get('Key')
            # check for pattern matching if specified
            if filepattern is not None:
                matched = re.match(filepattern, key)
                is_match = bool(matched)
            # read in the CSV
            if is_match and ".csv" in key:
                if not quiet:
                    logging.info(f"Reading in {key} from S3")
                file_body = client.get_object(Bucket=bucket, Key=key).get('Body')
                df = pd.read_csv(io.BytesIO(file_body.read()))
                logging.info(f"adding {key} to file_ls")
                outlist.append((key, df))
    return outlist