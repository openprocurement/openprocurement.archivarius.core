# -*- coding: utf-8 -*-
import datetime
import unittest
import uuid
from gevent import sleep
from gevent.queue import Queue
from mock import MagicMock, patch
from munch import munchify
from openprocurement_client.exceptions import (
    InvalidResponse,
    RequestFailed,
    ResourceNotFound as RNF
)
from openprocurement.archivarius.core.workers import ArchiveWorker


class TestArchiveWorker(unittest.TestCase):

    worker_config = {
        'resource': 'tenders',
        'client_inc_step_timeout': 0.1,
        'client_dec_step_timeout': 0.02,
        'drop_threshold_client_cookies': 1.5,
        'worker_sleep': 0.3,
        'retry_default_timeout': 0.4,
        'retries_count': 5,
        'queue_timeout': 0.2,
        'bulk_save_limit': 1,
        'bulk_save_interval': 1
    }

    log_dict = {
        'not_actual_docs_count': 0,
        'update_documents': 0,
        'save_documents': 0,
        'add_to_retry': 0,
        'droped': 0,
        'skiped': 0,
        'add_to_resource_items_queue': 0,
        'exceptions_count': 0,
        'not_found_count': 0,
        'moved_to_public_archive': 0,
        'dumped_to_secret_archive': 0,
        'archived': 0
    }

    def tearDown(self):
        self.worker_config['resource'] = 'tenders'
        self.worker_config['client_inc_step_timeout'] = 0.1
        self.worker_config['client_dec_step_timeout'] = 0.02
        self.worker_config['drop_threshold_client_cookies'] = 1.5
        self.worker_config['worker_sleep'] = 3
        self.worker_config['retry_default_timeout'] = 1
        self.worker_config['retries_count'] = 5
        self.worker_config['queue_timeout'] = 3

        for key in self.log_dict:
            self.log_dict[key] = 0

    def test_init(self):
        worker = ArchiveWorker()
        self.assertEqual(worker.exit, False)
        self.assertEqual(worker.update_doc, False)
        self.assertEqual(worker.db, None)
        self.assertEqual(worker.archive_db, None)
        self.assertEqual(worker.secret_archive_db, None)
        self.assertEqual(worker.config, None)
        self.assertEqual(worker.log_dict, None)
        self.assertEqual(worker.api_clients_queue, None)
        self.assertEqual(worker.resource_items_queue, None)
        self.assertEqual(worker.retry_resource_items_queue, None)
        self.assertGreater(datetime.datetime.now().isoformat(), worker.start_time.isoformat())

    def test_add_to_retry_queue(self):
        retry_items_queue = Queue()
        worker = ArchiveWorker(config_dict=self.worker_config,
                               retry_resource_items_queue=retry_items_queue,
                               log_dict=self.log_dict)
        retry_item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat(),
            'resource': 'tenders'
        }
        self.assertEqual(retry_items_queue.qsize(), 0)
        self.assertEqual(worker.log_dict['add_to_retry'], 0)

        # Add to retry_resource_items_queue
        worker.add_to_retry_queue(retry_item)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(retry_items_queue.qsize(), 1)
        self.assertEqual(worker.log_dict['add_to_retry'], 1)
        retry_item_from_queue = retry_items_queue.get()
        self.assertEqual(retry_item_from_queue['retries_count'], 1)
        self.assertEqual(retry_item_from_queue['timeout'],
                         worker.config['retry_default_timeout'] * 2)

        # Add to retry_resource_items_queue with status_code '429'
        worker.add_to_retry_queue(retry_item, status_code=429)
        retry_item_from_queue = retry_items_queue.get()
        self.assertEqual(retry_item_from_queue['retries_count'], 1)
        self.assertEqual(retry_item_from_queue['timeout'],
                         worker.config['retry_default_timeout'] * 2)

        # Drop from retry_resource_items_queue
        retry_item['retries_count'] = 6
        self.assertEqual(worker.log_dict['droped'], 0)
        worker.add_to_retry_queue(retry_item)
        self.assertEqual(worker.log_dict['droped'], 1)
        self.assertEqual(retry_items_queue.qsize(), 0)

        del worker

    def test__get_api_client_dict(self):
        api_clients_queue = Queue()
        client = MagicMock()
        client_dict = {'client': client, 'request_interval': 0}
        api_clients_queue.put(client_dict)

        # Success test
        worker = ArchiveWorker(api_clients_queue=api_clients_queue,
                               config_dict=self.worker_config,
                               log_dict=self.log_dict)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, client_dict)

        # Empty queue test
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client, None)
        del worker

    def test__get_resource_item_from_queue(self):
        items_queue = Queue()
        item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat(),
            'resource': 'tenders'}
        items_queue.put(item)

        # Success test
        worker = ArchiveWorker(resource_items_queue=items_queue, config_dict=self.worker_config,
                               log_dict=self.log_dict)
        self.assertEqual(worker.resource_items_queue.qsize(), 1)
        resource_item = worker._get_resource_item_from_queue()
        self.assertEqual(resource_item, item)
        self.assertEqual(worker.resource_items_queue.qsize(), 0)

        # Empty queue test
        resource_item = worker._get_resource_item_from_queue()
        self.assertEqual(resource_item, None)
        del worker

    @patch('openprocurement_client.client.TendersClient')
    def test__action_resource_item_from_cdb(self, mock_api_client):
        item = {
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.utcnow().isoformat(),
            'resource': 'tenders'
        }
        api_clients_queue = Queue()
        api_clients_queue.put({
            'client': mock_api_client,
            'request_interval': 0.02})
        retry_queue = Queue()
        return_dict = {
            'data': {
                'id': item['id'],
                'dateModified': datetime.datetime.utcnow().isoformat()
            }
        }
        mock_api_client.get_resource_dump.return_value = return_dict
        worker = ArchiveWorker(api_clients_queue=api_clients_queue,
                               config_dict=self.worker_config,
                               retry_resource_items_queue=retry_queue,
                               log_dict=self.log_dict)

        # Success test
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(api_client['request_interval'], 0.02)
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._action_resource_item_from_cdb(api_client, item)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 0)
        self.assertEqual(public_item, return_dict['data'])

        # InvalidResponse
        mock_api_client.get_resource_dump.side_effect = InvalidResponse('invalid response')
        self.assertEqual(self.log_dict['exceptions_count'], 0)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 0)
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._action_resource_item_from_cdb(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.log_dict['exceptions_count'], 1)
        self.assertEqual(worker.log_dict['add_to_retry'], 1)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 1)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)

        # RequestFailed status_code=429
        mock_api_client.get_resource_dump.side_effect = RequestFailed(
            munchify({'status_code': 429}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        self.assertEqual(api_client['request_interval'], 0)
        public_item = worker._action_resource_item_from_cdb(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.log_dict['exceptions_count'], 2)
        self.assertEqual(worker.log_dict['add_to_retry'], 2)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 2)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        self.assertEqual(api_client['request_interval'], worker.config['client_inc_step_timeout'])

        # RequestFailed status_code=429 with drop cookies
        api_client['request_interval'] = 2
        public_item = worker._action_resource_item_from_cdb(api_client, item)
        sleep(api_client['request_interval'])
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 3)
        self.assertEqual(worker.log_dict['add_to_retry'], 3)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 3)

        # RequestFailed with status_code not equal 429
        mock_api_client.get_resource_dump.side_effect = RequestFailed(
            munchify({'status_code': 404}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._action_resource_item_from_cdb(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(api_client['request_interval'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 4)
        self.assertEqual(worker.log_dict['add_to_retry'], 4)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 4)

        # ResourceNotFound
        mock_api_client.get_resource_dump.side_effect = RNF(
            munchify({'status_code': 404}))
        api_client = worker._get_api_client_dict()
        self.assertEqual(worker.api_clients_queue.qsize(), 0)
        public_item = worker._action_resource_item_from_cdb(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(worker.api_clients_queue.qsize(), 1)
        self.assertEqual(api_client['request_interval'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 4)
        self.assertEqual(worker.log_dict['add_to_retry'], 4)
        self.assertEqual(worker.log_dict['not_found_count'], 1)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 4)

        # Exception
        api_client = worker._get_api_client_dict()
        mock_api_client.get_resource_dump.side_effect = Exception('text except')
        public_item = worker._action_resource_item_from_cdb(api_client, item)
        self.assertEqual(public_item, None)
        self.assertEqual(api_client['request_interval'], 0)
        self.assertEqual(worker.log_dict['exceptions_count'], 5)
        self.assertEqual(worker.log_dict['add_to_retry'], 5)
        sleep(worker.config['retry_default_timeout'] * 2)
        self.assertEqual(worker.retry_resource_items_queue.qsize(), 5)

        del worker

    @patch('openprocurement_client.client.TendersClient')
    def test__run(self, mock_api_client):
        queue = Queue()
        retry_queue = Queue()
        api_clients_queue = Queue()
        api_client_dict = {
            'client': mock_api_client,
            'request_interval': 0
        }
        queue_resource_item = {
            'resource': 'tenders',
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.now().isoformat(),
            '_rev': '1-' + uuid.uuid4().hex
        }
        resource_item = {
            'resource': 'tenders',
            'id': uuid.uuid4().hex,
            'dateModified': datetime.datetime.now().isoformat(),
            '_rev': '1-' + uuid.uuid4().hex
        }
        archive_doc = {
            'id': resource_item['id'],
            'dateModified': datetime.datetime.now().isoformat(),
            '_rev': '1-' + uuid.uuid4().hex,
            'rev': '1-' + uuid.uuid4().hex
        }
        db = MagicMock()
        archive_db = MagicMock()
        secret_archive_db = MagicMock()
        bridge = ArchiveWorker(config_dict=self.worker_config, log_dict=self.log_dict,
                               resource_items_queue=queue, retry_resource_items_queue=retry_queue,
                               api_clients_queue=api_clients_queue, db=db, archive_db=archive_db,
                               secret_archive_db=secret_archive_db)

        # Try get item from resource items queue
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 0)
        self.assertEqual(bridge.log_dict['add_to_retry'], 0)

        # Get resource from edge db
        bridge.db.get.side_effect = [Exception('DB exception'), None, resource_item, resource_item,
                                     resource_item, resource_item, resource_item, resource_item,
                                     resource_item, resource_item, resource_item, resource_item]
        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 1)
        self.assertEqual(bridge.log_dict['add_to_retry'], 1)

        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 1)
        self.assertEqual(bridge.log_dict['add_to_retry'], 1)

        resource_item['dateModified'] = datetime.datetime.now().isoformat()
        bridge.archive_db.get.side_effect = [Exception('Archive DB exception'), None,
                                             munchify(archive_doc), munchify(archive_doc),
                                             munchify(archive_doc), munchify(archive_doc),
                                             munchify(archive_doc), munchify(archive_doc),
                                             munchify(archive_doc)]
        bridge.archive_db.save = MagicMock()

        # Put resource to public db
        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 2)
        self.assertEqual(bridge.log_dict['add_to_retry'], 2)

        # Try get api client from clients queue
        queue.put(queue_resource_item)
        bridge._get_api_client_dict = MagicMock(side_effect=[None, api_client_dict, api_client_dict,
                                                             api_client_dict, None, api_client_dict,
                                                             api_client_dict, api_client_dict,
                                                             api_client_dict, api_client_dict,
                                                             api_client_dict])
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 2)
        self.assertEqual(bridge.log_dict['add_to_retry'], 3)

        # Try get resource item dump from cdb
        resource_item['_rev'] = '1-' + uuid.uuid4().hex
        secret_doc = {
            'id': resource_item['id']
        }
        bridge._action_resource_item_from_cdb = MagicMock(side_effect=[Exception('From CDB exception'),
                                                                       secret_doc, secret_doc,
                                                                       secret_doc, Exception('Delete'),
                                                                       secret_doc, secret_doc,
                                                                       secret_doc, secret_doc])
        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 3)
        self.assertEqual(bridge.log_dict['add_to_retry'], 4)

        bridge.secret_archive_db.get.side_effect = [Exception('Secret DB exception'), None, secret_doc,
                                                    secret_doc, secret_doc, secret_doc, secret_doc]
        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 4)
        self.assertEqual(bridge.log_dict['add_to_retry'], 5)

        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 4)
        self.assertEqual(bridge.log_dict['add_to_retry'], 5)

        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 5)
        self.assertEqual(bridge.log_dict['add_to_retry'], 5)

        # Delete resource from edge db
        queue.put(queue_resource_item)
        bridge.db.save.side_effect = [True, Exception('Delete from edge')]
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 5)
        self.assertEqual(bridge.log_dict['add_to_retry'], 5)

        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 6)
        self.assertEqual(bridge.log_dict['add_to_retry'], 5)

    def test_shutdown(self):
        worker = ArchiveWorker()
        self.assertEqual(worker.exit, False)
        worker.shutdown()
        self.assertEqual(worker.exit, True)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestArchiveWorker))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
