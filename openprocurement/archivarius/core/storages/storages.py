from boto.s3.connection import S3Connection
from simplejson import dumps, loads
from ConfigParser import NoOptionError
from uuid import UUID
from logging import getLogger
from functools import partial
from openprocurement.archivarius.core.db import prepare_couchdb

logger = getLogger(__name__)


def config_get(config, opt):
    try:
        config.get('main', opt)
    except NoOptionError:
        return ''


class S3Storage(object):

    def __init__(self, connection, bucket):
        self.connection = connection
        self.bucket = bucket

    def _parse_key(self, doc_id):
        return '/'.join([format(i, 'x') for i in UUID(doc_id).fields])

    def save(self, data):
        _id = data.get('id') if 'id' in data else data.get('_id')
        path = self._parse_key(_id)
        bucket = self.connection.get_bucket(self.bucket)
        key = bucket.new_key(path)
        key.set_contents_from_string(dumps(data))
        key.set_acl('private')
        key.set_metadata('Content-Type', 'application/json')

    def get(self, key):
        bucket = self.connection.get_bucket(self.bucket)
        if '/' in key:
            path = key
        else:
            try:
                path = self._parse_key(key)
            except ValueError:
                return None
        if path not in bucket:
            return None
        key = bucket.get_key(path)
        data = loads(key.get_contents_as_string(path))
        return data


def s3(bridge):
    aws_params = {}
    for name, value in bridge.config.items('main'):
        if name[:3] != 's3.' or 'bucket' in name:
            continue
        aws_params[name[3:]] = value
    connection = S3Connection(**aws_params)
    storage = S3Storage(connection, config_get(bridge.config, 's3.bucket'))
    setattr(bridge, 'secret_archive', storage)


def couch(bridge):
    url = getattr(bridge, 'couch_url')
    default_db_name = getattr(bridge, 'db_archive_name')
    name = '{}_{}'.format(default_db_name, 'secret')
    setattr(bridge, 'secret_archive', prepare_couchdb(url, name, logger))
