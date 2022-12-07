# Gigasheet

import requests
import json
import os
import urllib.parse
import time
import base64


expected_filter_key = '_cnf_'

_api_key_env = 'GIGASHEET_API_KEY'
_auth_header = 'X-GIGASHEET-TOKEN'
_gigasheet_api_base_url = "https://api.gigasheet.com"
_file_wait_status = ('uploading', 'loading', 'processing')
_file_success_status = ('processed')


# Will read API key from env variable GIGASHEET_API_KEY if not provided on init
# API calls throw an error if they fail
class Gigasheet(object):
   
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

    def upload_url(self, url, name_after_upload):
        body = {
            'url': url,
            'name': name_after_upload,
            }
        resp = self._post('/upload/url', body)
        return resp['Handle']
    
    def upload_file(self, path_on_disk, name_after_upload):
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
        resp = self._post(f'/rename/{handle}', body)
    
    def list_saved_filters(self):
        resp = self._post('/filter-templates')
    
    def share(self, handle, recipients):
        if not recipients:
            return
        url = f'/file/{handle}/share/file'
        body = {
            'emails': recipients,
            'link': f'https://app.gigasheet.com/spreadsheet/shared/{handle}' # TODO: this should not be here
            }
        self._put(url, body)
    
    def wait_for_file_to_finish(self, handle, seconds_between_polls=1.0, max_tries=1000):
        if not handle:
            raise ValueError('Empty value for handle')
        success = False
        i = 0
        status = None
        for i in range(max_tries):
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
            time.sleep(seconds_between_polls)
        if not success:
            raise RuntimeError(f'Handle {handle} still not done after {i} tries, last status was: {status}')

    def get_rows(self, handle, start_row, end_row, filter_model={}):
        if not handle:
            raise ValueError('Empty value for handle')
        url = f'/file/{handle}/filter'
        data = {
            'startRow':start_row,
            'endRow':end_row,
            'filterModel':filter_model,
        }
        if (not (
            filter_model == {} or
            (len(filter_model) == 1 and list(filter_model.keys())[0] == expected_filter_key)
        )):
            raise ValueError(f'Invalid filter model, should be empty dict or dict with one key {_expected_filter_key}')
        return self._post(url, data)
