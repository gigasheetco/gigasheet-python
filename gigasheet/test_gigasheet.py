import unittest
from unittest.mock import MagicMock


from gigasheet.gigasheet import Gigasheet


_mock_api_key = 'mock_gigasheet_api_key'
_mock_handle = 'd0966d5f_b668_44c5_8536_ae1f89ca8d37'


def giga_with_mock():
    g = Gigasheet(api_key=_mock_api_key)
    g._post = MagicMock()
    g._get = MagicMock()
    g._put = MagicMock()
    g._delete = MagicMock()
    return g


class UploadTest(unittest.TestCase):

    def test_upload_file(self):
        g = giga_with_mock()
        name = 'mock file upload'
        test_file = 'gigasheet/testdata/sample-local-upload.csv'
        expected_bytes = 'bm90LHJlYWwKdGVzdCxmaWxl'  # base64 encoded file contents of test file
        g.upload_file(test_file, name)
        expected_body = {
            'name': name,
            'contents': expected_bytes,
            'parentDirectory': ''
        }
        g._post.assert_called_with('/upload/direct', expected_body)

    def test_upload_file_append(self):
        g = giga_with_mock()
        name = 'mock file upload'
        test_file = 'gigasheet/testdata/sample-local-upload.csv'
        expected_bytes = 'bm90LHJlYWwKdGVzdCxmaWxl'  # base64 encoded file contents of test file
        g.upload_file(test_file, name, _mock_handle)
        expected_body = {
            'name': name,
            'contents': expected_bytes,
            'parentDirectory': '',
            'targetHandle': _mock_handle
        }
        g._post.assert_called_with('/upload/direct', expected_body)

    def test_upload_url(self):
        g = giga_with_mock()
        name = 'mock url upload'
        url = 'https://gigasheet.com/nothing'
        g.upload_url(url, name)
        expected_body = {
            'name': name,
            'url': url,
        }
        g._post.assert_called_with('/upload/url', expected_body)

    def test_upload_url_append(self):
        g = giga_with_mock()
        name = 'mock file upload'
        url = 'https://gigasheet.com/nothing'
        g.upload_url(url, name, _mock_handle)
        expected_body = {
            'name': name,
            'url': url,
            'targetHandle': _mock_handle
        }
        g._post.assert_called_with('/upload/url', expected_body)


class ColumnsTest(unittest.TestCase):

    def test_map_columns(self):
        g = Gigasheet(api_key=_mock_api_key)
        fake_resp = [
            {'Name': '#', 'FieldType': 'UInt64', 'Id': 'A', 'AtIndex': 0, 'Hidden': False},
            {'Name': 'A', 'FieldType': 'EmailAddress', 'Id': 'B', 'AtIndex': 1, 'Hidden': False},
            {'Name': 'A - Domain', 'FieldType': 'String', 'Id': 'C', 'AtIndex': 2, 'Hidden': False},
            {'Name': 'A', 'FieldType': 'String', 'Id': 'D', 'AtIndex': 3, 'Hidden': False},
        ]
        g._get = MagicMock(return_value=fake_resp)
        res = g.column_ids_for_names(_mock_handle, ['A - Domain'])
        g._get.assert_called_with(f'/dataset/{_mock_handle}/columns', params={'showHidden': True})
        self.assertEqual(res, ['C'])
        self.assertRaises(ValueError, lambda: g.column_ids_for_names(_mock_handle, ['A']))
        self.assertRaises(ValueError, lambda: g.column_ids_for_names(_mock_handle, ['X']))


class DescriptionTest(unittest.TestCase):

    def test_set_description(self):
        g = giga_with_mock()
        desc = "a great description"
        g.set_description(_mock_handle, desc)
        expected_body = {'note': desc}
        g._put.assert_called_with(f'/dataset/{_mock_handle}/note', expected_body)


if __name__ == '__main__':
    unittest.main()
