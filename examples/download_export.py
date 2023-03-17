"""
This script is for getting an export from Gigasheet.

It takes a sheet handle and generates an export. Then you can either download it to a file or print an S3 presigned URL to download on another system.

See README for setup instructions.

For usage instructions, run:
python3 download_export.py --help 

"""


import argparse
import urllib
import os

from gigasheet import gigasheet


def main():
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--handle',
            help='sheet handle to export',
            required=True)
    parser.add_argument('--output-dir',
            help='specify local directory to save the file, or omit to just print the S3 presigned URL to download on another system',
            required=False)
    args = parser.parse_args()

    # Don't overwrite existing files
    output_path = None
    if args.output_dir:
        output_path = os.path.join(args.output_dir, 'export.zip')
        if os.path.isfile(output_path):
            raise ValueError(f'Rename existing file or choose new directory, file already exists: {output_path}')

    # Get Gigasheet client
    giga = gigasheet.Gigasheet()

    # Create an export
    export_handle = giga.create_export_current_state(args.handle)
    print(f'Unique ID of export: {export_handle}')
    print('Waiting for export to complete...')
    giga.wait_for_file_to_finish(export_handle)

    # Get export URL
    url = giga.download_export(export_handle)
    print('Presigned URL:')
    print(url)  # print on new line because this URL is really long

    # Save to disk, if specified
    if args.output_dir:
        print('Downloading file...')
        urllib.request.urlretrieve(url, output_path)
        print(f'Saved to: {output_path}')


if __name__ == '__main__':
    main()
