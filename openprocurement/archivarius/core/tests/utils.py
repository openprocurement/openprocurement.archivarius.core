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
from libnacl.public import Box
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
tender.doc_type.lower.return_value = 'tender'
tender.dateModified.isoformat.return_value = datetime.now().isoformat()
tender.serialize.return_value = dict(
    id=uuid.uuid4().hex,
    dateModified=datetime.now().isoformat(),
    rev='1-{}'.format(uuid.uuid4().hex),
    doc_type='Tender'
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
        cls.config.registry.decr_box = Box("b"*32, "c"*32)
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

    @patch('libnacl.crypto_box_keypair')
    def test_dump_resource(self, mock_crypto_box_keypair):
        request = MagicMock()
        request.registry.arch_pubkey = 'c' * 32
        mock_crypto_box_keypair.return_value = ["a"*32, "b"*32]
        context = {
            'id': uuid.uuid4().hex,
            'rev': '1-{}'.format(uuid.uuid4().hex),
            'dateModified': datetime.now().isoformat(),
            'doc_type': 'Tenders'
        }
        request.context.serialize.return_value = context
        dump = dump_resource(request)
        res, key = dump['item'], dump['pubkey']
        decrypt_box = Box("b"*32, "c"*32)
        decrypted_data = decrypt_box.decrypt(b64decode(res))
        decrypted_data = json.loads(decrypted_data)
        self.assertNotEqual(res, json.dumps(context))
        self.assertEqual(decrypted_data, context)

    @patch('libnacl.crypto_box_keypair')
    def test_dump(self, mock_crypto_box_keypair):
        response = self.app.get('/tenders/abc/dump', status=403)
        self.assertEqual(response.status, '403 Forbidden')
        self.app.authorization = ('Basic', ('archivarius', ''))
        self.app.app.registry.arch_pubkey = "c"*32
        mock_crypto_box_keypair.return_value = ["a"*32, "b"*32]
        response = self.app.get('/tenders/abc/dump')
        encrypted_dump = response.json['data']['tender']
        archive_box = self.config.registry.decr_box
        decrypted_data = archive_box.decrypt(b64decode(encrypted_dump['item']))
        decrypted_data = json.loads(decrypted_data)
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(decrypted_data, tender.serialize.return_value)

    @patch('libnacl.crypto_box_keypair')
    @patch('openprocurement.api.utils.error_handler')
    @patch('openprocurement.api.utils.context_unpack')
    def test_delete(self, mock_error_handler, mock_context_unpack, mock_crypto_box_keypair):
        response = self.app.delete('/tenders/abc/dump', status=403)
        self.assertEqual(response.status, '403 Forbidden')
        self.app.authorization = ('Basic', ('archivarius', ''))
        self.app.app.registry.arch_pubkey = "c"*32
        mock_crypto_box_keypair.return_value = ["a"*32, "b"*32]
        response = self.app.delete('/tenders/abc/dump')
        encrypted_dump = response.json['data']['tender']
        archive_box = self.config.registry.decr_box
        decrypted_data = archive_box.decrypt(b64decode(encrypted_dump['item']))
        decrypted_data = json.loads(decrypted_data)
        self.assertEqual(response.status, '200 OK')
        self.assertEqual(decrypted_data, tender.serialize.return_value)
