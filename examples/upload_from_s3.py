"""
This script is a simplified version of upload_and_share_cli.py.

It is meant to illustrate how to import into Gigasheet using the Amazon Web Services boto3 SDK and then give edit access to one or more collaborators.

For more flexible command-line usage for importing and sharing with Gigasheet, look at import_and_share_cli.py instead.

To use this script, you must set the following three environment variables:
GIGASHEET_API_KEY
AWS_ACCESS_KEY_ID
AWS_SECRET_ACCESS_KEY

For the GIGASHEET_API_KEY variable, check the Authentication section of the README of this repo.

For the AWS variables, obtain those from your AWS administrator.

Usage:

python3 upload_from_s3.py --s3-bucket <s3 bucket> --s3-key <s3 key> --recipients <recipient 1> <recipient 2> ...

"""


import boto3
import argparse
import sys

from gigasheet import gigasheet


LINK_EXPIRATION_SECONDS=3600


def make_s3_presigned_url(bucket: str, key: str) -> str:
    """make_s3_presigned_url

    Parameters:
        bucket: the S3 bucket
        key: the S3 key you want to load to Gigasheet

    Returns:
        a presigned S3 URL for that object
    """
    s3Client = boto3.client('s3')
    res = s3Client.generate_presigned_url('get_object', Params = {'Bucket': bucket, 'Key': key}, ExpiresIn = LINK_EXPIRATION_SECONDS)
    return res


def upload_and_share(url: str, name: str, recipients: list) -> str:
    """upload_and_share

    Parameters:
        url (str): the URL to import
        name (str): the name to call the file in Gigasheet
        recipients (list): a list of email addresses to add as editors

    Returns:
        str: the unique identifier for the imported file
    """
    giga = gigasheet.Gigasheet()
    sheet = giga.upload_url(url, name)
    giga.wait_for_file_to_finish(sheet)
    if recipients:
        # The True flag here gives write access in addition to read access.
        giga.share(sheet, recipients, True)
    return sheet


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--s3-bucket',
            help='S3 bucket to load from',
            required=True)
    parser.add_argument('--s3-key',
            help='S3 object key to load from',
            required=True)
    parser.add_argument('--recipients',
            help='Email addresses to share with, can provide multiple values to this parameter separated by spaces',
            default=[],
            nargs='*')
    args = parser.parse_args()

    # Get presigned URL to send to Gigasheet
    url = make_s3_presigned_url(args.s3_bucket, args.s3_key)
    print('Obtained presigned URL:')
    print(url)

    # Import into Gigasheet using the presigned URL and share
    sheet = upload_and_share(url, args.s3_key, args.recipients)
    print('Success! Gigasheet URL:')
    print(gigasheet.Gigasheet.get_sheet_url(sheet))


if __name__ == '__main__':
    main()
