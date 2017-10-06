.. _tutorial:

Tutorial
========

Introduction
------------

CDB Archiving means transfer of the completed procurement procedures to an
archival database on a separate server --- CDB copies with read-only
operations allowed and a separate endpoint.

Elements that are going to be transferred to the archival database:

1. procurement procedures in terminal status (`complete`, `unsuccessful`,
`cancelled`) after 100-day stay on the production database.

2. contracts in terminal status (`terminated`) after 100-day stay on the
production base.

3. plans that had been modified 455 (365+90) days for the last time before
archiving (`dateModified` field in the database).

The public part of the procedure will be transferred into the public database
of the archive, `Public Archive`; the private part of the procedure in the
encrypted form will be put into the secret database of the archive,
`Secret Archive`. Access to the archive database is possible in read-only
mode, so elements transferred to the archival database can't be edited anymore.
Archiving procedure is conducted weekly or on request.


Getting archived resource from the sandbox
------------------------------------------

Let's try getting tender already archived from the sandbox:

.. include:: tutorial/tender-archived-sandbox.http
   :code:


The same getting procedure is valid for other resources: `plans` and
`contracts`.

.. index:: Archived resource in sandbox


Getting archived resource from the `Archivarius`
------------------------------------------------

Let's try getting tender already archived from the archive:

.. include:: tutorial/tender-archived-archivarius.http
   :code:

The same getting procedure is valid for other resources: `plans` and
`contracts`.

Only public resource data are derived by the request above. Getting secret
data can be retrieved and decrypted only in manual mode.

.. index:: Archived resource in archivarius
