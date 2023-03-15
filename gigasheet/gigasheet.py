# Gigasheet

import requests
import json
import os
import urllib.parse
import time
import base64
from enum import IntEnum


expected_filter_key = '_cnf_'

_api_key_env = 'GIGASHEET_API_KEY'
_auth_header = 'X-GIGASHEET-TOKEN'
_gigasheet_api_base_url = "https://api.gigasheet.com"
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

    def upload_url(self, url: str, name_after_upload: str) -> str:
        body = {
            'url': url,
            'name': name_after_upload,
            }
        resp = self._post('/upload/url', body)
        return resp['Handle']
    
    def upload_file(self, path_on_disk: str, name_after_upload: str) -> str:
        with open(path_on_disk, 'rb') as fid:
            contents = fid.read()
        body = {
                'name': name_after_upload,
                'contents': str(base64.b64encode(contents), 'UTF-8'),
                'parentDirectory': '',
            }
        resp = self._post('/upload/direct', body)
        return resp['Handle']
    
    def info(self, handle):
        return self._get(f'/dataset/{handle}')

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
    
    def wait_for_file_to_finish(self, handle, seconds_between_polls=1.0, max_tries=1000):
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
            except:
                continue
            status = info.get('Status')
            if status == _file_success_status:
                success = True
                break
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

    def get_columns(self, handle):
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

    

class SharePermission(IntEnum):
    READ = 0
    WRITE = 1
