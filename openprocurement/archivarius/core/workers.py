# -*- coding: utf-8 -*-
from datetime import datetime
from gevent import Greenlet
from gevent import spawn, sleep
import logging
import logging.config
from openprocurement_client.exceptions import (
    InvalidResponse,
    RequestFailed,
    ResourceNotFound
)

logger = logging.getLogger(__name__)


class ArchiveWorker(Greenlet):

    def __init__(self, api_clients_queue=None, resource_items_queue=None,
                 db=None, archive_db=None, secret_archive_db=None, config_dict=None, retry_resource_items_queue=None,
                 log_dict=None):
        Greenlet.__init__(self)
        self.exit = False
        self.update_doc = False
        self.db = db
        self.archive_db = archive_db
        self.secret_archive_db = secret_archive_db
        self.config = config_dict
        self.log_dict = log_dict
        self.api_clients_queue = api_clients_queue
        self.resource_items_queue = resource_items_queue
        self.retry_resource_items_queue = retry_resource_items_queue
        self.start_time = datetime.now()

    def add_to_retry_queue(self, resource_item, status_code=0):
        timeout = resource_item.get('timeout') or self.config['retry_default_timeout']
        retries_count = resource_item.get('retries_count') or 0
        if status_code != 429:
            resource_item['timeout'] = timeout * 2
            resource_item['retries_count'] = retries_count + 1
        else:
            resource_item['timeout'] = timeout
            resource_item['retries_count'] = retries_count
        if resource_item['retries_count'] > self.config['retries_count']:
            self.log_dict['droped'] += 1
            logger.critical('{} {} reached limit retries count {} and'
                            ' droped from retry_queue.'.format(
                                resource_item['resource'].title(),
                                resource_item['id'],
                                self.config['retries_count']))
        else:
            self.log_dict['add_to_retry'] += 1
            spawn(self.retry_resource_items_queue.put,
                  resource_item, timeout=timeout)
            logger.info('Put {} {} to \'retries_queue\''.format(
                resource_item['resource'], resource_item['id']))

    def _get_api_client_dict(self):
        if not self.api_clients_queue.empty():
            api_client_dict = self.api_clients_queue.get(
                timeout=self.config['queue_timeout'])
            logger.debug('Got api_client {}'.format(
                api_client_dict['client'].session.headers['User-Agent']
            ))
            return api_client_dict
        else:
            return None

    def _get_resource_item_from_queue(self):
        if not self.resource_items_queue.empty():
            queue_resource_item = self.resource_items_queue.get(
                timeout=self.config['queue_timeout'])
            logger.debug('Get {} {} from main queue.'.format(queue_resource_item['resource'], queue_resource_item['id']))
            return queue_resource_item
        else:
            return None

    def _action_resource_item_from_cdb(self, api_client_dict, queue_resource_item, action="get_resource_dump"):
        try:
            logger.debug('Request interval {} sec. for client {}'.format(
                api_client_dict['request_interval'],
                api_client_dict['client'].session.headers['User-Agent']))
            # Resource object from api server
            resource_item = getattr(api_client_dict['client'], action)(queue_resource_item['id'], queue_resource_item['resource']).get('data')
            logger.debug('Recieved from API {}: {} '.format(queue_resource_item['resource'], queue_resource_item['id']))
            if api_client_dict['request_interval'] > 0:
                api_client_dict['request_interval'] -= self.config['client_dec_step_timeout']
            self.api_clients_queue.put(api_client_dict)
            return resource_item
        except InvalidResponse as e:
            self.api_clients_queue.put(api_client_dict)
            logger.error('Error while getting {} {} from public with '
                         'status code: {}'.format(
                             queue_resource_item['resource'],
                             queue_resource_item['id'],
                             e.status_code))
            self.add_to_retry_queue(queue_resource_item)
            self.log_dict['exceptions_count'] += 1
            return None
        except RequestFailed as e:
            if e.status_code == 429:
                if api_client_dict['request_interval'] > self.config['drop_threshold_client_cookies']:
                    api_client_dict['client'].session.cookies.clear()
                    api_client_dict['request_interval'] = 0
                else:
                    api_client_dict['request_interval'] += self.config['client_inc_step_timeout']
                spawn(self.api_clients_queue.put, api_client_dict,
                      timeout=api_client_dict['request_interval'])
            else:
                self.api_clients_queue.put(api_client_dict)
            logger.error('Request failed while getting {} {} from public'
                         ' with status code {}: '.format(
                             queue_resource_item['resource'],
                             queue_resource_item['id'], e.status_code))
            self.add_to_retry_queue(queue_resource_item, status_code=e.status_code)
            self.log_dict['exceptions_count'] += 1
            return None  # request failed
        except ResourceNotFound as e:
            logger.error('Resource not found {} at cdb: {}. {}'.format(
                queue_resource_item['resource'], queue_resource_item['id'], e.message))
            self.log_dict['not_found_count'] += 1
            self.api_clients_queue.put(api_client_dict)
            return None  # not found
        except Exception as e:
            self.api_clients_queue.put(api_client_dict)
            logger.error('Error while getting resource item {} {} from'
                         ' cdb {}: '.format(
                             queue_resource_item['resource'],
                             queue_resource_item['id'], e.message))
            self.add_to_retry_queue(queue_resource_item)
            self.log_dict['exceptions_count'] += 1
            return None

    def _run(self):
        while not self.exit:
            # Try get item from resource items queue
            queue_resource_item = self._get_resource_item_from_queue()
            if queue_resource_item is None:
                break

            # Get resource from edge db
            try:
                resource_item_doc = self.db.get(queue_resource_item['id'])
            except Exception as e:
                self.add_to_retry_queue(queue_resource_item)
                logger.error('Error while getting resource item from couchdb: '
                             '{}'.format(e.message))
                self.log_dict['exceptions_count'] += 1
                continue

            if not resource_item_doc:
                continue
            resource_item_rev = resource_item_doc['_rev']

            # Put resource to public db
            try:
                archive_item_doc = self.archive_db.get(queue_resource_item['id'])
                if archive_item_doc is None:
                    del resource_item_doc['_rev']
                    self.archive_db.save(resource_item_doc)
                elif archive_item_doc['dateModified'] < resource_item_doc['dateModified']:
                    resource_item_doc['_rev'] = archive_item_doc.rev
                    self.archive_db.save(resource_item_doc)
            except Exception as e:
                self.add_to_retry_queue(queue_resource_item)
                logger.error('Error while putting resource item to couchdb: '
                             '{}'.format(e.message))
                self.log_dict['exceptions_count'] += 1
                continue
            self.log_dict['moved_to_public_archive'] += 1

            # Try get api client from clients queue
            api_client_dict = self._get_api_client_dict()
            if api_client_dict is None:
                self.add_to_retry_queue(queue_resource_item)
                sleep(self.config['worker_sleep'])
                continue

            # Try get resource item dump from cdb
            try:
                secret_doc = self._action_resource_item_from_cdb(api_client_dict, queue_resource_item)
            except Exception as e:
                self.api_clients_queue.put(api_client_dict)
                self.add_to_retry_queue(queue_resource_item)
                logger.error('Error while getting resource item dump from cdb: {}'.format(e.message))
                self.log_dict['exceptions_count'] += 1
                continue

            # Put secret resource to secret db
            if secret_doc:
                try:
                    archive_item_doc = self.secret_archive_db.get(queue_resource_item['id'])
                    if archive_item_doc is None:
                        self.secret_archive_db.save({'_id': queue_resource_item['id'], 'data': secret_doc})
                    #elif archive_item_doc['dateModified'] < resource_item_doc['dateModified']:
                        #resource_item_doc['_rev'] = archive_item_doc.rev
                        #self.archive_db.save(resource_item_doc)
                except Exception as e:
                    self.add_to_retry_queue(queue_resource_item)
                    logger.error('Error while putting resource item to secret couchdb: '
                                 '{}'.format(e.message))
                    self.log_dict['exceptions_count'] += 1
                    continue
            self.log_dict['dumped_to_secret_archive'] += 1

            # Try get api client from clients queue
            api_client_dict = self._get_api_client_dict()
            if api_client_dict is None:
                self.add_to_retry_queue(queue_resource_item)
                sleep(self.config['worker_sleep'])
                continue

            # Try delete resource item from cdb
            try:
                secret_doc = self._action_resource_item_from_cdb(api_client_dict, queue_resource_item, 'delete_resource_dump')
            except Exception as e:
                self.api_clients_queue.put(api_client_dict)
                self.add_to_retry_queue(queue_resource_item)
                logger.error('Error while deleting resource item dump from cdb: {}'.format(e.message))
                self.log_dict['exceptions_count'] += 1
                continue

            # Delete resource from edge db
            try:
                resource_item_doc = self.db.save({'_id': queue_resource_item['id'], '_rev': resource_item_rev, '_deleted': True})
            except Exception as e:
                self.add_to_retry_queue(queue_resource_item)
                logger.error('Error while getting resource item from couchdb: '
                             '{}'.format(e.message))
                self.log_dict['exceptions_count'] += 1
                continue
            self.log_dict['archived'] += 1

    def shutdown(self):
        self.exit = True
        logger.info('Worker complete his job.')
