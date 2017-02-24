# -*- coding: utf-8 -*-
import unittest
import uuid
from ConfigParser import ConfigParser
from couchdb import Server
from logging import getLogger
from mock import patch, MagicMock
from munch import munchify
from openprocurement_client.exceptions import RequestFailed
from socket import error

from openprocurement.archivarius.core.bridge import (
    prepare_couchdb,
    ConfigError,
    DEFAULTS,
    WORKER_CONFIG,
    ArchivariusBridge
)

logger = getLogger(__name__)

class TestBridge(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.couchdb_url = 'http://127.0.0.1:5984'
        cls.config = ConfigParser()
        cls.config.add_section('main')
        cls.config.set('main', 'resources_api_server', 'http://localhost')


    def test_prepare_couchdb(self):
        # Database don't exist.
        db_name = 'test_archivarius_' + uuid.uuid4().hex
        server = Server(self.couchdb_url)
        self.assertNotIn(db_name, server)
        db = prepare_couchdb(self.couchdb_url, db_name, logger)
        self.assertIn(db_name, db.name)

        # Database don't exist and create with exception
        del server[db_name]
        with patch('openprocurement.archivarius.core.bridge.Server.create') as mock_create:
            mock_create.side_effect = error('test error')
            with self.assertRaises(ConfigError) as e:
                prepare_couchdb(self.couchdb_url, db_name, logger)
            self.assertEqual(e.exception.message, None)

        self.assertNotIn(db_name, server)
        prepare_couchdb(self.couchdb_url, db_name, logger)
        self.assertIn(db_name, server)
        del server[db_name]

    def test_init(self):
        self.config.remove_option('main', 'resources_api_server')
        with self.assertRaises(ConfigError) as e:
            ArchivariusBridge(self.config)
        self.config.set('main', 'resources_api_server', 'asfd')
        self.assertEqual(e.exception.message, 'In config dictionary empty or missing'
                         ' \'resources_api_server\'')
        with self.assertRaises(ConfigError) as e:
            ArchivariusBridge(self.config)
        self.assertEqual(e.exception.message, 'Invalid \'resources_api_server\' url.')

        self.config.set('main', 'resources_api_server', 'http://localhost')
        self.config.set('main', 'retry_resource_items_queue_size', '-1')
        self.config.set('main', 'resource_items_queue_size', '-1')
        archivarius = ArchivariusBridge(self.config)
        self.assertEqual(archivarius.resource_items_queue.maxsize, None)
        self.assertEqual(archivarius.retry_resource_items_queue.maxsize, None)

        del archivarius
        self.config.set('main', 'retry_resource_items_queue_size', '1')
        self.config.set('main', 'resource_items_queue_size', '1')
        archivarius = ArchivariusBridge(self.config)
        self.assertEqual(archivarius.resource_items_queue.maxsize, 1)
        self.assertEqual(archivarius.retry_resource_items_queue.maxsize, 1)
        # import pdb; pdb.set_trace()

    @patch('openprocurement.archivarius.core.bridge.APIClient')
    def test_create_api_client(self, mock_APIClient):
        mock_APIClient.side_effect = [RequestFailed(), munchify({
            'session': {'headers': {'User-Agent': 'test.agent'}}
        })]
        archivarius = ArchivariusBridge(self.config)
        self.assertEqual(archivarius.api_clients_queue.qsize(), 0)
        self.assertEqual(archivarius.log_dict['exceptions_count'], 0)
        archivarius.create_api_client()
        self.assertEqual(archivarius.log_dict['exceptions_count'], 1)
        self.assertEqual(archivarius.api_clients_queue.qsize(), 1)

        del archivarius

    @patch('openprocurement.archivarius.core.bridge.APIClient')
    def test_fill_api_clients_queue(self, mock_APIClient):
        bridge = ArchivariusBridge(self.config)
        self.assertEqual(bridge.api_clients_queue.qsize(), 0)
        bridge.fill_api_clients_queue()
        self.assertEqual(bridge.api_clients_queue.qsize(),
                         bridge.workers_min)

    @patch('openprocurement.archivarius.core.bridge.ifilter')
    def test_fill_resource_items_queue(self, mock_ifilter):
        mock_ifilter.return_value = [munchify({'id': uuid.uuid4().hex}),
                                     munchify({'id': uuid.uuid4().hex})]
        bridge = ArchivariusBridge(self.config)
        bridge.resources['tenders'] = {'view_path': 'path', 'filter': MagicMock()}
        self.assertEqual(bridge.resource_items_queue.qsize(), 0)
        self.assertEqual(bridge.log_dict['add_to_resource_items_queue'], 0)
        bridge.fill_resource_items_queue('tenders')
        self.assertEqual(bridge.resource_items_queue.qsize(), 2)
        self.assertEqual(bridge.log_dict['add_to_resource_items_queue'], 2)

    @patch('openprocurement.archivarius.core.bridge.spawn')
    @patch('openprocurement.archivarius.core.bridge.ArchiveWorker.spawn')
    @patch('openprocurement.archivarius.core.bridge.APIClient')
    def test_gevent_watcher(self, mock_APIClient, mock_riw_spawn, mock_spawn):
        bridge = ArchivariusBridge(self.config)
        self.assertEqual(bridge.workers_pool.free_count(),
                         bridge.workers_max)
        self.assertEqual(bridge.retry_workers_pool.free_count(),
                         bridge.retry_workers_max)
        bridge.resource_items_queue.put('item')
        bridge.retry_resource_items_queue.put('item')
        bridge.gevent_watcher()
        self.assertEqual(bridge.workers_pool.free_count(),
                         bridge.workers_max - bridge.workers_min)
        self.assertEqual(bridge.retry_workers_pool.free_count(),
                         bridge.retry_workers_max - bridge.retry_workers_min)
        del bridge


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestBridge))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
