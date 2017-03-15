import logging
from simplejson import dumps, loads
from uuid import UUID


class ContentExists(ValueError):
    pass


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
        if path in bucket:
            raise ContentExists
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
