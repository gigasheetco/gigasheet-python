# Gigasheet

import requests
import json
import os
import urllib.parse
import collections
import time
import base64
from enum import IntEnum


expected_filter_key = '_cnf_'

_api_key_env = 'GIGASHEET_API_KEY'
_auth_header = 'X-GIGASHEET-TOKEN'
_gigasheet_api_base_url = "https://api.gigasheet.com"
_gigasheet_ui_base_url = "https://app.gigasheet.com"
_file_wait_status = ('uploading', 'loading', 'processing')
_file_success_status = ('processed')


# Will read API key from env variable GIGASHEET_API_KEY if not provided on init
# API calls throw an error if they fail
class Gigasheet(object):
    enrichment_data_types = {
        'email-format-check': 'EMAIL',
        }
   
    def __init__(self, api_key=None):
        if api_key:
            self.api_key = api_key
        else:
            self.api_key = os.getenv(_api_key_env)
        if not self.api_key:
            raise ValueError(f'No API key, provide in constructor or set env {_api_key_env}')
        self._headers = {
            _auth_header:self.api_key,
            'Content-type': 'application/json',  # Required by Gigasheet API
        }

    @staticmethod
    def get_sheet_url(handle: str) -> str:
        """get_sheet_url

        Get the URL to view a sheet in the Gigasheet web application.

        Parameters:
            handle (str): sheet handle

        Returns:
            str: the URL of that sheet in the Gigasheet web application

        See also get_handle_from_url for converting the opposite direction.
        """
        return f'{_gigasheet_ui_base_url}/spreadsheet/id/{handle}'
    
    @staticmethod
    def get_handle_from_url(url: str) -> str:
        """get_handle_from_url

        Get the handle of a sheet from the sheet URL.

        Parameters:
            url (str): the URL of a sheet

        Returns:
            str: the handle of the sheet

        See also get_sheet_url for converting the opposite direction.
        """
        if not url.startswith(f'{_gigasheet_ui_base_url}/spreadsheet'):
            raise ValueError('Must be a complete URL of a sheet in the Gigasheet UI')
        parts = urllib.parse.urlparse(url).path.split('/')
        if len(parts) < 4:
            raise ValueError('No handle found in URL')
        handle = parts[3]
        if not handle:
            raise ValueError('No handle found in URL')
        return handle
    
    def upload_url(self, url: str, name_after_upload: str, append_to_handle: str = None) -> str:
        """upload_url

        Upload into Gigasheet from a world-readable URL.

        Parameters:
            url (str): the URL to upload from
            name_after_upload (str): the name after the upload is done, must be non-enpty but is ignored if successfully appended
            append_to_handle (str): optionally specify an existing file handle to append records

        Returns
            str: sheet handle that uniquely identifies the uploaded file in Gigasheet
        """
        body = {
            'url': url,
            'name': name_after_upload,
            }
        if append_to_handle:
            body['targetHandle'] = append_to_handle
        resp = self._post('/upload/url', body)
        return resp['Handle']
    
    def upload_file(self, path_on_disk: str, name_after_upload: str, append_to_handle: str = None) -> str:
        """upload_file

        Upload a local file into Gigasheet.

        This uses a single http connection, so depending on the speed of your internet connection, you may not be able to upload large files. For large files, consider putting your data on a cloud storage and using upload_url with a presigned link instead.

        Parameters:
            path_on_disk (str): the path to the local file to upload
            name_after_upload (str): the name after the upload is done, must be non-enpty but is ignored if successfully appended
            append_to_handle (str): optionally specify an existing file handle to append records
        
        Returns
            str: sheet handle that uniquely identifies the uploaded file in Gigasheet
        """
        with open(path_on_disk, 'rb') as fid:
            contents = fid.read()
        body = {
                'name': name_after_upload,
                'contents': str(base64.b64encode(contents), 'UTF-8'),
                'parentDirectory': '',
                
            }
        if append_to_handle:
            body['targetHandle'] = append_to_handle
        resp = self._post('/upload/direct', body)
        return resp['Handle']

    def info(self, handle: str) -> dict:
        """info

        Get metadata about a sheet.

        Includes things like filename, column types, last modified, and file state.

        Parameters:
            handle(str): sheet handle

        Returns:
            dict: metadata about the sheet
        """
        return self._get(f'/dataset/{handle}')

    def create_export(self, handle: str, state: dict = {}, name: str = 'export.csv', folder_handle: str = '') -> str:
        """create_export

        Create an export for a sheet, can be used with download_export

        Parameters:
            handle (str): sheet handle to export
            state (dict): the state of the sheet to apply when exporting, look at ClientState field in return value from info()
            name (str): filename of the file that will be created with the data
            folder_handle (str): handle of Gigasheet directory to place export into

        Returns:
            str: handle of the export in Gigasheet, use with download_export
        """
        body = {
                'filename': name,
                'folderHandle': folder_handle,
                'gridState': state,
                }
        resp = self._post(f'/dataset/{handle}/export', body)
        return resp['handle']

    def create_export_current_state(self, handle: str, name: str = 'export.csv', folder_handle: str = '') -> str:
        """create_export_current_state

        Same as create_export but uses current file state when exporting

        Parameters:
            handle (str): sheet handle to export
            state (dict): the state of the sheet to apply when exporting, see get_state
            name (str): filename of the file that will be created with the data
            folder_handle (str): handle of Gigasheet directory to place export into

        Returns:
            str: handle of the export in Gigasheet, use with download_export
        """
        state = self.info(handle).get('ClientState', {})
        return self.create_export(handle, state, name, folder_handle)
    
    def download_export(self, export_handle: str) -> str:
        """download_export

        Obtain an S3 presigned URL for a completed export.

        Use create_export to get an export handle and then wait for it to finish with wait_for_file_to_finish.

        Parameters:
            export_handle(str): handle of an export, must be already finished

        Returns:
            str: an S3 presigned URL for the export file
        """
        return self._get(f'dataset/{export_handle}/download-export')['presignedUrl']

    def column_ids_for_names(self, handle: str, column_names: list) -> list:
        """column_ids_for_names

        Maps column names to column IDs.
        
        Input column names must exist and be unique or this raises a ValueError.

        Params:
            handle (str): The handle of the sheet to get column IDs.
            column_names (list): List of strings of column names to map to IDs.
        
        Returns:
            list: A list of strings of column IDs corresponding to the input names.
        """
        cols = self.get_columns(handle)
        out = []
        name_to_ids = collections.defaultdict(list)
        for c in cols:
            name_to_ids[c['Name']].append(c['Id'])
        for n in column_names:
            c = name_to_ids[n]
            if not c:
                raise ValueError(f'No column found with name: {n}')
            if len(c) > 1:
                raise ValueError(f'Multiple matches for column name: {n}')
            out.append(c[0])
        return out
    
    def deduplicate_rows(self, handle: str, column_ids: list, sort_model: object):
        """deduplicate_rows

        Removes duplicate rows.

        Example sort model: [{"colId": "B", "sort": "desc"}]

        Params:
            handle (str): The sheet handle to remove duplicate rows from.
            column_ids (str): The column IDs to deduplicate, multiple columns are treated as a compound key.
            sort_model (object): Sort model to use when deduplicating, first row will be kept.
        """
        body = {
            'columns':column_ids,
            'sortModel':sort_model
        }
        self._delete(f'/dataset/{handle}/deduplicate-rows', body)
    
    def count_rows(self, handle: str, filter_model: object = None) -> int:
        """count_rows
        
        Query a sheet and return row count, optionally with a filter.

        Uses the the regular row method underneath, see get_rows for more.

        Params:
            handle (str): The sheet handle to count the rows of
            filter_model (object): Optional filter model to apply before counting

        Returns:
            int: The row count 
        """
        resp = self.get_rows(handle, 0, 1, filter_model)
        return resp['lastRow']

    def rename(self, handle, new_name):
        body = {'uuid':handle, 'filename':new_name}
        return self._post(f'/rename/{handle}', body)
    
    def list_saved_filters(self):
        return self._get('/filter-templates')

    def share(self, handle, recipients, with_write=False, message=''):
        url = f'/file/{handle}/share/file'
        permissions = [SharePermission.READ]
        if with_write:
            permissions.append(SharePermission.WRITE)
        body = {
                'emails':recipients,
                'permissions':permissions,
                'message':message,
            }
        self._put(url, body)

    def unshare(self, handle):
        self._share_set_public(handle, False)
    
    def wait_for_file_to_finish(self, handle: str, deletion_is_success: bool = False, seconds_between_polls: float = 1.0, max_tries: int = 1000):
        """wait_for_file_to_finish
        
        Poll a handle until it is in a successful state, or raise a RuntimeError.

        Params:
            handle (str): The handle to poll.
            deletion_is_success (bool): Some jobs delete after completion, so set this to true to count deletion as success.
            seconds_between_polls (float): Seconds to wait between polling.
            max_tries (int): Number of times to poll before assumming the job was a failure.
        """
        if not handle:
            raise ValueError('Empty value for handle')
        success = False
        i = 0
        status = None
        for i in range(max_tries):
            if i != 0:
                time.sleep(seconds_between_polls)
            try:
                info = self.info(handle)
            except requests.exceptions.HTTPError as e:
                # This block is here because some operations, such as appending to a sheet, create transient sheets in Gigasheet.
                # Those transient sheets are deleted after the job is done, and the poll will receive a 400 Bad Request in response when that happens.
                # Thus, we provide the deletion_is_success flag on this method and check for 400 responses that say "deleted", treating that as done.
                # This is a somewhat odd situation driven by the implementation on the backend, so we may change how we handle it in a future version.
                if deletion_is_success:
                    resp = e.response
                    if resp.status_code == 400 and 'deleted' in resp.text:
                        return
                else:
                    continue
            except Exception:
                # Ignore errors unless we are checking for deletion as success.
                continue
            status = info.get('Status')
            if status == _file_success_status:
                return
            if status not in _file_wait_status:
                raise RuntimeError(f'Bad status on handle {handle}: {status}')
        if not success:
            raise RuntimeError(f'Handle {handle} still not done after {i} tries, last status was: {status}')

    def get_rows(self, handle, start_row, end_row, filter_model=None):
        if not handle:
            raise ValueError('Empty value for handle')
        url = f'/file/{handle}/filter'
        data = {
            'startRow':start_row,
            'endRow':end_row,
            'filterModel':filter_model,
        }
        if (not (
            filter_model is None or
            filter_model == {} or
            (len(filter_model) == 1 and list(filter_model.keys())[0] == expected_filter_key)
        )):
            raise ValueError(f'Invalid filter model, should be empty dict or dict with one key {expected_filter_key}')
        return self._post(url, data)

    def gets(self, handle):
        if not handle:
            raise ValueError('Empty value for handle')
        return self._get(f'/dataset/{handle}/columns')

    def get_filter_model_for_saved_filter_on_sheet(self, sheet_handle, saved_filter_handle):
        if not sheet_handle:
            raise ValueError('Empty value for sheet handle')
        if not saved_filter_handle:
            raise ValueError('Empty value for saved filter handle')
        url = f'/filter-templates/{saved_filter_handle}/on-sheet/{sheet_handle}'
        return self._get(url)

    def get_rows_with_saved_filter(self, sheet_handle, saved_filter_handle, start_row, end_row):
        resp = self.get_filter_model_for_saved_filter_on_sheet(sheet_handle, saved_filter_handle)
        filter_model = resp['filterModel']
        return self.get_rows(sheet_handle, start_row, end_row, filter_model)

    def enrich_builtin(self, handle, column_id, enrichment_service_provider, filter_model=None):
        if not handle:
            raise ValueError('Empty value for sheet handle')
        if not enrichment_service_provider in self.enrichment_data_types:
            return ValueError(f'Unknown enrichment service provider: {enrichment_service_provider}')
        data = {
                'filterModel':filter_model,
                'enrichments':[{
                    'provider':enrichment_service_provider,
                    'type':self.enrichment_data_types[enrichment_service_provider],
                    'key':None
                    }] 
                }
        url = f'/enrichments/{handle}/{column_id}'
        return self._post(url, data)

    def enrich_email_format(self, handle, column_id, filter_model=None):
        return self.enrich_builtin(handle, column_id, 'email-format-check', filter_model)

    def _url(self, endpoint):
        return urllib.parse.urljoin(_gigasheet_api_base_url, endpoint)
    
    def _after(self, resp):
        resp.raise_for_status()
        return resp.json()

    def _post(self, endpoint, data):
        return self._after(requests.post(self._url(endpoint), headers=self._headers, data=json.dumps(data)))
    
    def _put(self, endpoint, data):
        return self._after(requests.put(self._url(endpoint), headers=self._headers, data=json.dumps(data)))

    def _get(self, endpoint):
        return self._after(requests.get(self._url(endpoint), headers=self._headers))
    
    def _delete(self, endpoint, data):
        return self._after(requests.delete(self._url(endpoint), headers=self._headers, data=json.dumps(data)))


class SharePermission(IntEnum):
    READ = 0
    WRITE = 1
