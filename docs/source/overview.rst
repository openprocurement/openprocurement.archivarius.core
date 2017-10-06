Overview
========

Workflow
--------

.. graphviz::

    digraph ARCHIVING {
        {rank=same p e cbd}
        p[label="Public API"];e[label="EDGE"];pa[label="Public Archive"];
        sa[label="Secret Archive"];cbd[label="CDB"];
        cbd -> p -> e [label="0) sync"];
        e -> pa [label="1) rules"];
        cbd -> sa [label="2) dump"];
        sa -> cbd [label="3) delete"];
        sa -> e [label="4) delete"];
    }


`sync`
  звичайна синхронізація в EDGE публічних даних

`rules`
  перенесення публічних даних в архів згідно з правилами архівування

`dump`
  збереження шифрованих внутрішніх структур даних в архіві секретних даних

`delete`
  видалення структури з ЦБД


Documentation of related packages
---------------------------------

* `OpenProcurement API <http://api-docs.openprocurement.org/en/latest/>`_

* `Open tender procedure (OpenUA) <http://openua.api-docs.openprocurement.org/en/latest/>`_

* `Open tender procedure with publication in English (OpenEU) <http://openeu.api-docs.openprocurement.org/en/latest/>`_

* `Reporting, negotiation procurement procedure and negotiation procedure for the urgent need  <http://limited.api-docs.openprocurement.org/en/latest/>`_

* `Defense open tender <http://defense.api-docs.openprocurement.org/en/latest/>`_

* `Contracting API interface to OpenProcurement database <http://contracting.api-docs.openprocurement.org/en/latest/>`_

* `Relocation API <http://relocation.api-docs.openprocurement.org/en/latest/>`_
