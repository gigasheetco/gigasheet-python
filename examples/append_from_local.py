"""
Script showing how to append to an existing sheet.

Sheet columns must match. Can be used to append with no additional logic, or you may specify column names to use for only keeping new rows or upserting over old values.

See README for setup instructions.
"""

import argparse

from gigasheet import gigasheet


def append_from_file(handle: str, input_file_path: str, unique_identifier_col_names: list, upsert: bool, description: str):
    # Get a client instance
    giga = gigasheet.Gigasheet()
    # Check the deduplicate columns are okay before appending
    dedupe_col_ids = None
    sort_col_id = None
    if unique_identifier_col_names:
        # Convert column names to IDs, also raises error if there are multiple columns with the same name
        dedupe_col_ids = giga.column_ids_for_names(handle, unique_identifier_col_names)
        # For dedupe sort we will use the builtin "#" column
        sort_col_id = giga.column_ids_for_names(handle, ['#'])[0]
    # Append to the file
    print(f'Appending to handle {handle} with current row count: {giga.count_rows(handle)}')
    name_if_failed = f'failed append to {handle}' # this is only used if we have an error while trying to append
    job_handle = giga.upload_file(input_file_path, name_if_failed, handle)
    print(f'Uploading as job: {job_handle}')
    giga.wait_for_file_to_finish(job_handle, True) # append jobs are deleted after completion
    print(f'Uploaded data parsed, new combined row count: {giga.count_rows(handle)}')
    # Deduplicate, if requested, sorting according to upsert parameter
    if dedupe_col_ids:
        dedupe_behavior = 'keeping newer rows' if upsert else 'keeping older rows'
        print(f'Beginning deduplication {dedupe_behavior} on handle: {handle}')
        # Build a sort model using the builtin "#" column, sorting to either keep newest or oldest rows
        sort_model = [{'colId': sort_col_id, 'sort': 'desc' if upsert else 'asc'}]
        # Then deduplicate
        giga.deduplicate_rows(handle, dedupe_col_ids, sort_model)
        print(f'Deduplication finished, deduplicated row count: {giga.count_rows(handle)}')
    # Update description if requested
    if description is not None:
        giga.set_description(handle, description)
        print(f'Updated description')
    print()
    print(giga.get_sheet_url(handle))
    print()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--handle',
        help='Sheet handle to append or upsert onto',
        required=True)
    parser.add_argument('--input-file', 
        help='Path to local file to upsert onto the sheet',
        required=True)
    parser.add_argument('--deduplicate-by-col-names',
        help='Optional list of column names that taken together uniquely identify a row, if specified all duplicate rows in the sheet will be dropped including rows prior to append',
        action='append',
        default=[],
        required=False)
    parser.add_argument('--upsert',
        help='Optionally set this to upsert over existing rows instead of keeping old rows',
        required=False,
        default=False,
        action='store_true')
    parser.add_argument('--description',
        help='Optionally text string to set as the description on the updated sheet',
        required=False,
        default=None)
    args = parser.parse_args()
    if args.upsert and not args.deduplicate_by_col_names:
        raise ValueError('Must specify deduplicate_by_col_names to upsert')
    append_from_file(args.handle, args.input_file, args.deduplicate_by_col_names, args.upsert, args.description)
    
