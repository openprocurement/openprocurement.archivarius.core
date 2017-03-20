# -*- coding: utf-8 -*-
import datetime
import unittest
import uuid
import copy
import boto
from hashlib import md5
from gevent import sleep
from gevent.queue import Queue
from mock import MagicMock, patch
from munch import munchify
from boto.utils import (
    merge_headers_by_name,
    find_matching_headers,
    compute_md5
)

from openprocurement_client.exceptions import (
    InvalidResponse,
    RequestFailed,
    ResourceNotFound as RNF
)
from openprocurement.archivarius.core.workers import ArchiveWorker
from openprocurement.archivarius.core.storages import (
    S3Storage
)


NOT_IMPL = None


class MockAcl(object):

    def __init__(self, parent=NOT_IMPL):
        pass

    def startElement(self, name, attrs, connection):
        pass

    def endElement(self, name, value, connection):
        pass

    def to_xml(self):
        return '<mock_ACL_XML/>'


class MockKey(object):

    def __init__(self, bucket=None, name=None):
        self.bucket = bucket
        self.name = name
        self.data = None
        self.etag = None
        self.size = None
        self.closed = True
        self.content_encoding = None
        self.content_language = None
        self.content_type = None
        self.last_modified = 'Wed, 06 Oct 2010 05:11:54 GMT'
        self.BufferSize = 8192
        self.metadata = {}

    def get_contents_as_string(self, headers=None,
                               cb=NOT_IMPL, num_cb=NOT_IMPL,
                               torrent=NOT_IMPL,
                               version_id=NOT_IMPL,
                               response_headers=NOT_IMPL, encoding=NOT_IMPL):
        return self.data

    def set_contents_from_file(self, fp, headers=None, replace=NOT_IMPL,
                               cb=NOT_IMPL, num_cb=NOT_IMPL,
                               policy=NOT_IMPL, md5=NOT_IMPL,
                               res_upload_handler=NOT_IMPL):
        self.data = fp.read()
        self.set_etag()
        self.size = len(self.data)
        self._handle_headers(headers)

    def set_contents_from_string(self, s, headers=NOT_IMPL, replace=NOT_IMPL,
                                 cb=NOT_IMPL, num_cb=NOT_IMPL, policy=NOT_IMPL,
                                 md5=NOT_IMPL, reduced_redundancy=NOT_IMPL):
        self.data = copy.copy(s)
        self.set_etag()
        self.size = len(s)
        self._handle_headers(headers)

    def set_acl(self, acl_str, headers=None):
        pass

    def _handle_headers(self, headers):
        if not headers:
            return
        if find_matching_headers('Content-Encoding', headers):
            self.content_encoding = merge_headers_by_name('Content-Encoding',
                                                          headers)
        if find_matching_headers('Content-Type', headers):
            self.content_type = merge_headers_by_name('Content-Type', headers)
        if find_matching_headers('Content-Language', headers):
            self.content_language = merge_headers_by_name('Content-Language',
                                                          headers)

    def set_etag(self):
        """
        Set etag attribute by generating hex MD5 checksum on current
        contents of mock key.
        """
        m = md5()
        m.update(self.data)
        hex_md5 = m.hexdigest()
        self.etag = hex_md5

    def set_metadata(self, name, value):
        # Ensure that metadata that is vital to signing is in the correct
        # case. Applies to ``Content-Type`` & ``Content-MD5``.
        if name.lower() == 'content-type':
            self.metadata['Content-Type'] = value
        elif name.lower() == 'content-md5':
            self.metadata['Content-MD5'] = value
        else:
            self.metadata[name] = value

    def set_remote_metadata(self, metadata_plus, metadata_minus, preserve_acl,
                            headers=None):
        src_bucket = self.bucket
        metadata = self.metadata
        metadata.update(metadata_plus)
        for h in metadata_minus:
            if h in metadata:
                del metadata[h]
        rewritten_metadata = {}
        for h in metadata:
            if (h.startswith('x-goog-meta-') or h.startswith('x-amz-meta-')):
                rewritten_h = (h.replace('x-goog-meta-', '')
                               .replace('x-amz-meta-', ''))
            else:
                rewritten_h = h
            rewritten_metadata[rewritten_h] = metadata[h]
        metadata = rewritten_metadata
        src_bucket.copy_key(self.name, self.bucket.name, self.name,
                            metadata=metadata, preserve_acl=preserve_acl,
                            headers=headers)

    def copy(self, dst_bucket_name, dst_key, metadata=NOT_IMPL,
             reduced_redundancy=NOT_IMPL, preserve_acl=NOT_IMPL):
        dst_bucket = self.bucket.connection.get_bucket(dst_bucket_name)
        return dst_bucket.copy_key(dst_key, self.bucket.name, self.name, metadata)

    def get_metadata(self, name):
        if name.lower() == 'content-type':
            return self.metadata['Content-Type']
        elif name.lower() == 'content-md5':
            return self.metadata['Content-MD5']
        else:
            return self.metadata[name]

    def compute_md5(self, fp):
        """
        :type fp: file
        :param fp: File pointer to the file to MD5 hash.  The file pointer
                   will be reset to the beginning of the file before the
                   method returns.
        :rtype: tuple
        :return: A tuple containing the hex digest version of the MD5 hash
                 as the first element and the base64 encoded version of the
                 plain digest as the second element.
        """
        tup = compute_md5(fp)
        # Returned values are MD5 hash, base64 encoded MD5 hash, and file size.
        # The internal implementation of compute_md5() needs to return the
        # file size but we don't want to return that value to the external
        # caller because it changes the class interface (i.e. it might
        # break some code) so we consume the third tuple value here and
        # return the remainder of the tuple to the caller, thereby preserving
        # the existing interface.
        self.size = tup[2]
        return tup[0:2]


class MockBucket(object):

    def __init__(self, connection=None, name=None, key_class=NOT_IMPL):
        self.name = name
        self.keys = {}
        self.acls = {name: MockAcl()}
        # default object ACLs are one per bucket and not supported for keys
        self.def_acl = MockAcl()
        self.subresources = {}
        self.connection = connection
        self.logging = False

    def new_key(self, key_name=None):
        mock_key = MockKey(self, key_name)
        self.keys[key_name] = mock_key
        self.acls[key_name] = MockAcl()
        return mock_key

    def __contains__(self, key_name):
        return not (self.get_key(key_name) is None)

    def get_key(self, key_name, headers=NOT_IMPL, version_id=NOT_IMPL):
        # Emulate behavior of boto when get_key called with non-existent key.
        if key_name not in self.keys:
            return None
        return self.keys[key_name]

    def copy_key(self, new_key_name, src_bucket_name,
                 src_key_name, metadata=NOT_IMPL, src_version_id=NOT_IMPL,
                 storage_class=NOT_IMPL, preserve_acl=NOT_IMPL,
                 encrypt_key=NOT_IMPL, headers=NOT_IMPL, query_args=NOT_IMPL):
        src_key = self.connection.get_bucket(src_bucket_name).get_key(src_key_name)
        new_key = self.new_key(key_name=new_key_name)
        new_key.data = copy.copy(src_key.data)
        new_key.size = len(new_key.data)
        return new_key


class MockProvider(object):

    def __init__(self, provider):
        self.provider = provider

    def get_provider_name(self):
        return self.provider


class MockConnection(object):

    def __init__(self, aws_access_key_id=NOT_IMPL,
                 aws_secret_access_key=NOT_IMPL, is_secure=NOT_IMPL,
                 port=NOT_IMPL, proxy=NOT_IMPL, proxy_port=NOT_IMPL,
                 proxy_user=NOT_IMPL, proxy_pass=NOT_IMPL,
                 host=NOT_IMPL, debug=NOT_IMPL,
                 https_connection_factory=NOT_IMPL,
                 calling_format=NOT_IMPL,
                 path=NOT_IMPL, provider='s3',
                 bucket_class=NOT_IMPL):
        self.buckets = {}
        self.provider = MockProvider(provider)

    def create_bucket(self, bucket_name, headers=NOT_IMPL, location=NOT_IMPL,
                      policy=NOT_IMPL, storage_class=NOT_IMPL):
        if bucket_name in self.buckets:
            raise boto.exception.StorageCreateError(
                409, 'BucketAlreadyOwnedByYou',
                "<Message>Your previous request to create the named bucket "
                "succeeded and you already own it.</Message>")
        mock_bucket = MockBucket(name=bucket_name, connection=self)
        self.buckets[bucket_name] = mock_bucket
        return mock_bucket

    def get_bucket(self, bucket_name, validate=NOT_IMPL, headers=NOT_IMPL):
        if bucket_name not in self.buckets:
            raise boto.exception.StorageResponseError(404, 'NoSuchBucket', 'Not Found')
        return self.buckets[bucket_name]

    def get_all_buckets(self, headers=NOT_IMPL):
        return self.buckets.itervalues()

    def generate_url(self, expires_in, method, bucket='', key='', headers=None,
                     query_auth=True, force_http=False, response_headers=None,
                     expires_in_absolute=False, version_id=None):
        return 'http://s3/{}/{}'.format(bucket, key)


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

    @patch('openprocurement_client.client.TendersClient')
    def test_storage(self, mock_api_client):
        s3_key = 'key'
        s3_secret_key = 'secret'
        s3_bucket = 'bucket'
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
        conn = MockConnection(s3_key, s3_secret_key)
        conn.create_bucket(s3_bucket)
        secret_archive = S3Storage(conn, s3_bucket)
        bridge = ArchiveWorker(config_dict=self.worker_config, log_dict=self.log_dict,
                               resource_items_queue=queue, retry_resource_items_queue=retry_queue,
                               api_clients_queue=api_clients_queue, db=db, archive_db=archive_db,
                               secret_archive_db=secret_archive)
        bridge.db.get.side_effect = [resource_item, resource_item]
        bridge.archive_db.get.side_effect = [munchify(archive_doc), munchify(archive_doc)]
        bridge._get_api_client_dict = MagicMock(side_effect=[api_client_dict, api_client_dict])

        bridge.archive_db.save = MagicMock()

        # Try get resource item dump from cdb
        resource_item['_rev'] = '1-' + uuid.uuid4().hex
        secret_doc = {
            'id': resource_item['id']
        }
        bridge._action_resource_item_from_cdb = MagicMock(side_effect=[secret_doc, secret_doc])

        queue.put(queue_resource_item)
        bridge._run()
        self.assertEqual(bridge.log_dict['exceptions_count'], 0)
        self.assertEqual(bridge.log_dict['add_to_retry'], 0)
        data = bridge.secret_archive_db.get(queue_resource_item['id'])
        self.assertEqual(secret_doc, data.get('data'))

        # Test invalid key
        data = bridge.secret_archive_db.get('invalid')
        self.assertTrue(data is None)


def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(TestArchiveWorker))
    return suite


if __name__ == '__main__':
    unittest.main(defaultTest='suite')
