# -*- coding: utf-8 -*-
import unittest
import uuid
import json
from base64 import b64decode
from pyramid import testing
from openprocurement.api.auth import AuthenticationPolicy
from pyramid.authorization import ACLAuthorizationPolicy
from pyramid.renderers import JSONP
from pyramid.events import NewRequest, BeforeRender, ContextFound
from webtest import TestApp
from cornice.tests.support import CatchErrors
from datetime import datetime
from libnacl.secret import SecretBox
from openprocurement.api.utils import opresource, add_logging_context
from openprocurement.archivarius.core.utils import (
    Root,
    delete_resource,
    dump_resource,
    ArchivariusResource
)
from mock import MagicMock, patch
from munch import munchify

tender = MagicMock()
tender.serialize.return_value = dict(
    id=uuid.uuid4().hex,
    dateModified=datetime.now().isoformat(),
    rev='1-{}'.format(uuid.uuid4().hex),
    doc_type='Tenders'
)


def factory(request):
    root = Root(request)
    if not request.matchdict or not request.matchdict.get('tender_id'):
        return root
    request.validated['tender_id'] = request.matchdict['tender_id']
    tender.__parent__ = root
    request.validated['tender'] = request.validated['db_doc'] = 'Tenders'
    request.validated['tender_status'] = tender.status

    tender.return_value = munchify(tender.serialize.return_value)
    return tender


@opresource(name='Tender Archivarius',
            path='/tenders/{tender_id}/dump',
            description="Tender Archivarius View",
            factory=factory)
class TenderArchivariusResource(ArchivariusResource):

    pass


class TestUtils(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = testing.setUp()
        cls.config.add_renderer('jsonp', JSONP(param_name='callback'))
        cls.config.include("cornice")
        cls.authz_policy = ACLAuthorizationPolicy()

        cls.authn_policy = AuthenticationPolicy(
            'openprocurement/archivarius/core/tests/auth.ini', __name__)
        cls.config.set_authorization_policy(cls.authz_policy)
        cls.config.set_authentication_policy(cls.authn_policy)
        cls.config.registry.db = MagicMock()
        cls.config.registry.db.save.return_value = ['', '1-{}'.format(uuid.uuid4().hex)]
        cls.config.registry.server_id = uuid.uuid4().hex
        cls.config.registry.docservice_key = MagicMock()
        cls.config.context = munchify(tender.serialize.return_value)
        cls.config.registry.docservice_key.vk = 'a' * 32
        cls.config.add_subscriber(add_logging_context, NewRequest)
        cls.config.add_subscriber(MagicMock(), ContextFound)
        cls.config.add_subscriber(MagicMock(), NewRequest)
        cls.config.add_subscriber(MagicMock(), BeforeRender)
        cls.config.scan("openprocurement.archivarius.core.tests.utils")
        cls.app = TestApp(CatchErrors(cls.config.make_wsgi_app()))

    @classmethod
    def tearDownClass(self):
        testing.tearDown()

    def tearDown(self):
        self.app.authorization = ('Basic', ('', ''))

    def test_Root(self):
        request = munchify({'registry': {'db': 'database'}})
        root = Root(request)
        self.assertEqual(root.request, request)
        self.assertEqual(root.db, request.registry.db)

    def test_delete_resource(self):
        # Mock request
        request = MagicMock()
        db_doc = munchify({
            'id': uuid.uuid4().hex,
            'rev': '1-{}'.format(uuid.uuid4().hex),
            'dateModified': datetime.now(),
            'doc_type': 'Tenders'
        })
        request.context = db_doc

        db = MagicMock()
        db.save.return_value = ['', '1-{}'.format(uuid.uuid4().hex)]
        request.registry.db = db
        res = delete_resource(request)
        self.assertEqual(res, True)

    def test_dump_resource(self):
        request = MagicMock()
        context = {
            'id': uuid.uuid4().hex,
            'rev': '1-{}'.format(uuid.uuid4().hex),
            'dateModified': datetime.now().isoformat(),
            'doc_type': 'Tenders'
        }
        request.registry.docservice_key.vk = 'a' * 32
        request.context.serialize.return_value = context
        res = dump_resource(request)
        box = SecretBox('a' * 32)
        decrypted_data = box.decrypt(b64decode(res))
        decrypted_data = json.loads(decrypted_data)
        self.assertNotEqual(res, json.dumps(context))
        self.assertEqual(decrypted_data, context)

    def test_dump(self):
        response = self.app.get('/tenders/abc/dump', status=403)
        self.assertEqual(response.status, '403 Forbidden')
        self.app.authorization = ('Basic', ('archivarius', ''))
        response = self.app.get('/tenders/abc/dump')

        encrypted_dump = response.json['data']
        box = SecretBox('a' * 32)
        decrypted_data = box.decrypt(b64decode(encrypted_dump))
        decrypted_data = json.loads(decrypted_data)
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(decrypted_data, tender.serialize.return_value)

    @patch('openprocurement.api.utils.error_handler')
    @patch('openprocurement.api.utils.context_unpack')
    def test_delete(self, mock_error_handler, mock_context_unpack):
        response = self.app.delete('/tenders/abc/dump', status=403)
        self.assertEqual(response.status, '403 Forbidden')
        self.app.authorization = ('Basic', ('archivarius', ''))
        response = self.app.delete('/tenders/abc/dump')

        encrypted_dump = response.json['data']
        box = SecretBox('a' * 32)
        decrypted_data = box.decrypt(b64decode(encrypted_dump))
        decrypted_data = json.loads(decrypted_data)
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(decrypted_data, tender.serialize.return_value)
