# -*- coding: utf-8 -*-

from base64 import b64encode
from couchdb.http import ResourceConflict
from json import dumps
from libnacl.secret import SecretBox
from logging import getLogger
from openprocurement.api.utils import context_unpack, json_view, APIResource
from pyramid.security import Allow


LOGGER = getLogger(__package__)


class Root(object):
    __name__ = None
    __parent__ = None
    __acl__ = [
        (Allow, 'g:archivarius', 'dump_resource'),
        (Allow, 'g:archivarius', 'delete_resource'),
    ]

    def __init__(self, request):
        self.request = request
        self.db = request.registry.db


def delete_resource(request):
    db_doc = request.context
    resource = db_doc.doc_type.lower()
    try:
        _, rev = request.registry.db.save({'_id': db_doc.id, '_rev': db_doc.rev, '_deleted': True})
    except ResourceConflict, e:  # pragma: no cover
        request.errors.add('body', 'data', str(e))
        request.errors.status = 409
    except Exception, e:  # pragma: no cover
        request.errors.add('body', 'data', str(e))
    else:
        LOGGER.info('Deleted {} {}: dateModified {} -> None'.format(resource, db_doc.id, db_doc.dateModified.isoformat()),
                    extra=context_unpack(request, {'MESSAGE_ID': 'delete_resource'}, {'RESULT': rev}))
        return True


def dump_resource(request):
    docservice_key = getattr(request.registry, 'docservice_key', None)
    box = SecretBox(docservice_key.vk)
    data = request.context.serialize()
    json_data = dumps(data)
    encrypted_data = box.encrypt(json_data)
    return b64encode(encrypted_data)


class ArchivariusResource(APIResource):

    def __init__(self, request, context):
        super(ArchivariusResource, self).__init__(request, context)
        self.resource = request.context.doc_type.lower()

    @json_view(permission='dump_resource')
    def get(self):
        """Tender Dump
        """
        self.LOGGER.info('Dumped {} {}'.format(self.resource, self.context.id),
                         extra=context_unpack(self.request, {'MESSAGE_ID': '{}_dumped'.format(self.resource)}))
        return {'data': dump_resource(self.request)}

    @json_view(permission='delete_resource')
    def delete(self):
        """Delete tender
        """
        if delete_resource(self.request):
            self.LOGGER.info('Deleted {} {}'.format(self.resource, self.context.id),
                             extra=context_unpack(self.request, {'MESSAGE_ID': '{}_deleted'.format(self.resource)}))
            return {'data': dump_resource(self.request)}
