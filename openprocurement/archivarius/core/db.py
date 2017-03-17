# -*- coding: utf-8 -*-

from couchdb import Server, Session
from logging import getLogger
from socket import error


LOGGER = getLogger(__package__)


class ConfigError(Exception):
    pass


def prepare_couchdb(couch_url, db_name, logger=LOGGER):
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
