# coding=utf-8
import unittest
import uuid
from datetime import datetime
from mock import MagicMock, patch
from munch import munchify

from openprocurement.archivarius.core.client import APIClient

class TestAPIClient(unittest.TestCase):

    def setUp(self):
        self.tender_id = uuid.uuid4().hex
        self.date_modified = datetime.utcnow().isoformat()
        self.host_url = 'http://public.api-sandbox.openprocurement.org'
        self.tender_doc = '{{"id": "{}", "dateModified": "{}", "doc_type": "Tender"}}'.format(
        self.tender_id, self.date_modified)

    @patch('openprocurement.archivarius.core.client.APIClient.request')
    @patch('openprocurement_client.api_base_client.Session')
    def test_get_resource_dump(self, mock_session, mock_request):
        mock_session.headers = {}
        mock_request.return_value = munchify({
            'text': self.tender_doc,
            'status_code': 200
        })
        mock_request.status_code = 200
        api_client = APIClient(
            host_url=self.host_url,
            user_agent='test_agent',
            resource='RESOURCE',
            key=''
        )
        response = api_client.get_resource_dump(self.tender_id, 'tenders')
        # Check request method
        self.assertEqual(mock_request.call_args[0][0], 'GET')
        # Check request url
        self.assertEqual(mock_request.call_args[0][1], '{}/api/2.0/tenders/{}/dump'.format(
            self.host_url, self.tender_id))
        self.assertEqual(response.doc_type, 'Tender')
        self.assertEqual(response.id, self.tender_id)
        self.assertEqual(response.dateModified, self.date_modified)

    @patch('openprocurement.archivarius.core.client.APIClient.request')
    @patch('openprocurement_client.api_base_client.Session')
    def test_delete_resource_dump(self, mock_session, mock_request):
        mock_session.headers = {}
        mock_request.return_value = munchify({
            'text': self.tender_doc,
            'status_code': 200
        })
        mock_request.status_code = 200
        api_client = APIClient(
            host_url=self.host_url,
            user_agent='test_agent',
            resource='RESOURCE',
            key=''
        )
        response = api_client.delete_resource_dump(self.tender_id, 'tenders')
        # Check request method
        self.assertEqual(mock_request.call_args[0][0], 'DELETE')
        # Check request url
        self.assertEqual(mock_request.call_args[0][1], '{}/api/2.0/tenders/{}/dump'.format(
            self.host_url, self.tender_id))
        self.assertEqual(response.doc_type, 'Tender')
        self.assertEqual(response.id, self.tender_id)
        self.assertEqual(response.dateModified, self.date_modified)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestAPIClient))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
