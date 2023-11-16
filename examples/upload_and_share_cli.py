"""
Command-line utility for using Gigasheet Api to upload a file and share it.

See README for setup instructions.

If you find yourself piping data in the terminal, you can make a gigasheet pipe command by adding an alias to your bash_profile:
alias gigasheet='/path/to/gigasheet-python/examples/upload_and_share_cli.py --input-stdin'
"""

import argparse
import json
import os
import sys

from gigasheet import gigasheet


_default_name = f'Upload from {os.path.basename(__file__)}'


def main():
    # Get command-line arguments.
    parser = argparse.ArgumentParser(description='')
    parser_input = parser.add_mutually_exclusive_group(required=True)
    parser_input.add_argument('--input-url', 
        help='URL of file to upload to Gigasheet')
    parser_input.add_argument('--input-file', 
        help='Path to local file to upload to Gigasheet (max ~50MB depending on connection speed)')
    parser_input.add_argument('--input-stdin',
        help='Read stdin pipe to obtain file contents to upload to Gigasheet (max ~50MB depending on connection speed)',
        action='store_true',
        default=False)
    parser_input.add_argument('--input-handle',
        help='If a sheet was already uploaded, specify the handle to do rename and share on it instead of a new upload')
    parser.add_argument('--share-to',
        help='Email addresses to share with, repeat this flag to share to multiple recipients',
        required=False, 
        action='append',
        default=[])
    parser.add_argument('--share-write',
        help='Share with write permission as opposed to only read access',
        required=False,
        action='store_true',
        default=False)
    parser.add_argument('--share-message',
        help='Optional message to send with the share',
        required=False,
        default='')
    parser.add_argument('--api-key',
        help='Api key to use. Will use $GIGASHEET_API_KEY otherwise.',
        required=False, 
        default=None)
    parser.add_argument('--name',
        help='Rename sheet to this name. Intended for use with default example input.',
        required=False, 
        default=None)
    parser.add_argument('--description',
        help='Update the sheet description',
        required=False,
        default=None)
    parser.add_argument('--info',
        help='Print sheet info at the end of execution.',
        required=False,
        action='store_true',
        default=False)
    args = parser.parse_args()

    # Create an instance of the Gigasheet wrapper.
    giga = gigasheet.Gigasheet(args.api_key)

    # Get our sheet identifier and name, choosing sensible defaults if we can.
    sheet = None
    already_uploaded = False
    name = args.name
    if args.input_handle is not None:
        print(f'operating on handle: {args.input_handle}')
        sheet = args.input_handle
        already_uploaded = True
    elif args.input_url is not None:
        if name is None:
            name = _default_name
        print(f'attempting to load from URL: {args.input_url}')
        sheet = giga.upload_url(args.input_url, name)
    elif args.input_file is not None:
        if name is None:
            name = os.path.split(args.input_file)[-1]
        print(f'attempting to load from file: {args.input_file}')
        sheet = giga.upload_file(args.input_file, name)
    elif args.input_stdin:
        if name is None:
            name = _default_name
        sheet = giga.upload_filelike(sys.stdin.buffer, name)
    else:
        raise ValueError('Missing input parameter. Should be unreachable after argparse validation.')

    # If we didn't already have it uploaded, wait for it to finish uploading.
    if not already_uploaded:
        print(f'uploaded file: {sheet}')    
        # Wait for parse to complete.
        print('waiting for parsing to complete...')
        giga.wait_for_file_to_finish(sheet)
        print('sheet loaded')

    # If we did already have it uploaded, rename it.
    if already_uploaded and name:
        print(f'setting name: {name}')
        giga.rename(sheet, name)
        print('sheet renamed')

    # If requested, update the sheet description.
    if args.description is not None:
        giga.set_description(sheet, args.description)
        print('updated sheet description')

    # If requested, share the sheet.
    if args.share_to:
        giga.share(sheet, args.share_to, args.share_write, args.share_message)
        print(f'shared to {len(args.share_to)} recipients')

    # If requested, print sheet info at the end
    if args.info:
        info = giga.info(sheet)
        print(json.dumps(info, indent=2))


if __name__ == '__main__':
    main()
