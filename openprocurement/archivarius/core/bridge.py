# -*- coding: utf-8 -*-
from gevent import monkey
monkey.patch_all()

import logging
import logging.config
import os
import argparse
import uuid
from ConfigParser import ConfigParser, NoOptionError
from couchdb import Server, Session
from datetime import datetime
from functools import partial
from gevent import spawn, sleep
from gevent.pool import Pool
from gevent.queue import Queue
from itertools import ifilter
from openprocurement_client.exceptions import RequestFailed
from openprocurement.edge.utils import prepare_couchdb_views
from pkg_resources import iter_entry_points
from pytz import timezone
from socket import error
from urlparse import urlparse
from .workers import ArchiveWorker
from .client import APIClient

logger = logging.getLogger(__name__)
TZ = timezone(os.environ['TZ'] if 'TZ' in os.environ else 'Europe/Kiev')

WORKER_CONFIG = {
    'client_dec_step_timeout': 0.02,
    'client_inc_step_timeout': 0.1,
    'drop_threshold_client_cookies': 2,
    'queue_timeout': 3,
    'retries_count': 10,
    'retry_default_timeout': 3,
    'worker_sleep': 5,
}

DEFAULTS = {
    'api_key': '',
    'couch_url': 'http://127.0.0.1:5984',
    'db_name': 'edge_db',
    'db_archive_name': 'archive_db',
    'queues_controller_timeout': 60,
    'resource_items_queue_size': 10000,
    'retry_resource_items_queue_size': -1,
    'retry_workers_max': 2,
    'retry_workers_min': 1,
    'retry_workers_pool': 2,
    'user_agent': 'ArchivariusBridge',
    'watch_interval': 10,
    'workers_dec_threshold': 35,
    'workers_inc_threshold': 75,
    'workers_max': 3,
    'workers_min': 1,
}


class ConfigError(Exception):
    pass


def prepare_couchdb(couch_url, db_name, logger):
    server = Server(couch_url, session=Session(retry_delays=range(10)))
    try:
        if db_name not in server:
            db = server.create(db_name)
        else:
            db = server[db_name]
    except error as e:
        logger.error('Database error: {}'.format(e.message))
        raise ConfigError(e.strerror)

    #validate_doc = db.get(VALIDATE_BULK_DOCS_ID, {'_id': VALIDATE_BULK_DOCS_ID})
    #if validate_doc.get('validate_doc_update') != VALIDATE_BULK_DOCS_UPDATE:
        #validate_doc['validate_doc_update'] = VALIDATE_BULK_DOCS_UPDATE
        #db.save(validate_doc)
        #logger.info('Validate document update view saved.')
    #else:
        #logger.info('Validate document update view already exist.')
    return db


class ArchivariusBridge(object):

    """Archivarius Bridge"""

    def __init__(self, config):
        self.config = config
        self.workers_config = {}
        self.log_dict = {}
        self.bridge_id = uuid.uuid4().hex
        self.api_host = self.config_get('resources_api_server')
        self.api_version = self.config_get('resources_api_version')

        # Workers settings
        for key in WORKER_CONFIG:
            self.workers_config[key] = (self.config_get(key) or
                                        WORKER_CONFIG[key])

        # Init config
        for key in DEFAULTS:
            value = self.config_get(key)
            setattr(self, key, type(DEFAULTS[key])(value) if value else DEFAULTS[key])

        # Pools
        self.workers_pool = Pool(self.workers_max)
        self.retry_workers_pool = Pool(self.retry_workers_max)
        self.filter_workers_pool = Pool()

        # Queues
        self.api_clients_queue = Queue()
        if self.resource_items_queue_size == -1:
            self.resource_items_queue = Queue()
        else:
            self.resource_items_queue = Queue(self.resource_items_queue_size)
        if self.retry_resource_items_queue_size == -1:
            self.retry_resource_items_queue = Queue()
        else:
            self.retry_resource_items_queue = Queue(
                self.retry_resource_items_queue_size)

        # Default values for statistic variables
        for key in ('droped',
                    'add_to_resource_items_queue',
                    'add_to_retry',
                    'exceptions_count',
                    'not_found_count',
                    'archived',
                    'moved_to_public_archive',
                    'dumped_to_secret_archive',
                    ):
            self.log_dict[key] = 0

        if self.api_host != '' and self.api_host is not None:
            api_host = urlparse(self.api_host)
            if api_host.scheme == '' and api_host.netloc == '':
                raise ConfigError('Invalid \'resources_api_server\' url.')
        else:
            raise ConfigError('In config dictionary empty or missing'
                              ' \'resources_api_server\'')
        self.db = prepare_couchdb(self.couch_url, self.db_name, logger)
        self.archive_db = prepare_couchdb(self.couch_url, self.db_archive_name, logger)
        # TODO
        self.archive_db2 = prepare_couchdb(self.couch_url, self.db_archive_name + '_secret', logger)

        self.resources = {}
        for entry_point in iter_entry_points('openprocurement.archivarius.resources'):
            self.resources[entry_point.name] = {
                'filter': entry_point.load(),
                'view_path': '_design/{}/_view/by_dateModified'.format(entry_point.name)
            }
        for resource in self.resources:
            prepare_couchdb_views(self.couch_url + '/' + self.db_name, resource, logger)

    def create_api_client(self):
        client_user_agent = self.user_agent + '/' + self.bridge_id + '/' + uuid.uuid4().hex
        timeout = 0.1
        while True:
            try:
                api_client = APIClient(host_url=self.api_host,
                                       user_agent=client_user_agent,
                                       api_version=self.api_version,
                                       resource='RESOURCE',
                                       key=self.api_key)
                self.api_clients_queue.put({
                    'client': api_client,
                    'request_interval': 0})
                logger.info('Started api_client {}'.format(
                    api_client.session.headers['User-Agent']))
                break
            except RequestFailed as e:
                self.log_dict['exceptions_count'] += 1
                logger.error(
                    'Failed start api_client with status code {}'.format(
                        e.status_code))
                timeout = timeout * 2
                sleep(timeout)

    def fill_api_clients_queue(self):
        while self.api_clients_queue.qsize() == 0:
            self.create_api_client()

    def fill_resource_items_queue(self, resource):
        start_time = datetime.now(TZ)
        rows = self.db.iterview(self.resources[resource]['view_path'], 10 ** 3, include_docs=True)
        filter_func = partial(self.resources[resource]['filter'], time=start_time)
        for row in ifilter(filter_func, rows):
            self.resource_items_queue.put({
                'id': row.id,
                'resource': resource
            })
            self.log_dict['add_to_resource_items_queue'] += 1

    def queues_controller(self):
        while True:
            self.fill_api_clients_queue()
            #if self.workers_pool.free_count() > 0 and (self.resource_items_queue.qsize() > int((self.resource_items_queue_size / 100) * self.workers_inc_threshold)):
            if self.resource_items_queue.qsize() > 0 and self.workers_pool.free_count() > 0:
                w = ArchiveWorker.spawn(self.api_clients_queue,
                                        self.resource_items_queue,
                                        self.db, self.archive_db, self.archive_db2, self.workers_config,
                                        self.retry_resource_items_queue,
                                        self.log_dict)
                self.workers_pool.add(w)
                logger.info('Queue controller: Create main queue worker.')
            #elif self.resource_items_queue.qsize() < int((self.resource_items_queue_size / 100) * self.workers_dec_threshold):
            elif self.resource_items_queue.qsize() == 0:
                if len(self.workers_pool) > self.workers_min:
                    wi = self.workers_pool.greenlets.pop()
                    wi.shutdown()
                    logger.info('Queue controller: Kill main queue worker.')
            logger.info('Main resource items queue contains {} items'.format(self.resource_items_queue.qsize()))
            logger.info('Retry resource items queue contains {} items'.format(self.retry_resource_items_queue.qsize()))
            logger.info('Status: add to queue - {add_to_resource_items_queue}, add to retry - {add_to_retry}, moved to public archive - {moved_to_public_archive}, dumped to secret archive - {dumped_to_secret_archive}, archived - {archived}, exceptions - {exceptions_count}, not found - {not_found_count}'.format(**self.log_dict))
            sleep(self.queues_controller_timeout)

    def gevent_watcher(self):
        self.fill_api_clients_queue()
        if not self.resource_items_queue.empty() and len(self.workers_pool) < self.workers_min:
            w = ArchiveWorker.spawn(self.api_clients_queue,
                                    self.resource_items_queue,
                                    self.db, self.archive_db, self.archive_db2, self.workers_config,
                                    self.retry_resource_items_queue,
                                    self.log_dict)
            self.workers_pool.add(w)
            logger.info('Watcher: Create main queue worker.')
        if not self.retry_resource_items_queue.empty() and len(self.retry_workers_pool) < self.retry_workers_min:
            w = ArchiveWorker.spawn(self.api_clients_queue,
                                    self.retry_resource_items_queue,
                                    self.db, self.archive_db, self.archive_db2, self.workers_config,
                                    self.retry_resource_items_queue,
                                    self.log_dict)
            self.retry_workers_pool.add(w)
            logger.info('Watcher: Create retry queue worker.')

    def run(self):
        logger.info('Start Archivarius Bridge',
                    extra={'MESSAGE_ID': 'edge_bridge_start_bridge'})
        for resource in self.resources:
            self.filter_workers_pool.spawn(self.fill_resource_items_queue, resource=resource)
        spawn(self.queues_controller)
        while True:
            self.gevent_watcher()
            if len(self.filter_workers_pool) == 0 and len(self.workers_pool) == 0 and len(self.retry_workers_pool) == 0:
                break
            sleep(self.watch_interval)

    def config_get(self, name):
        try:
            return self.config.get('main', name)
        except NoOptionError:
            return


def main():
    parser = argparse.ArgumentParser(description='---- Archivarius Bridge ----')
    parser.add_argument('config', type=str, help='Path to configuration file')
    params = parser.parse_args()
    if os.path.isfile(params.config):
        config = ConfigParser()
        config.read([params.config])
        logging.config.fileConfig(params.config)
        ArchivariusBridge(config).run()


if __name__ == "__main__":
    main()
