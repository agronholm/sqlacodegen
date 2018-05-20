This is a tool that reads the structure of an existing database and generates the appropriate
SQLAlchemy model code, using the declarative style if possible.

This tool was written as a replacement for `sqlautocode`_, which was suffering from several issues
(including, but not limited to, incompatibility with Python 3 and the latest SQLAlchemy version).

.. _sqlautocode: http://code.google.com/p/sqlautocode/


Features
========

* Supports SQLAlchemy 0.8.x - 1.2.x
* Produces declarative code that almost looks like it was hand written
* Produces `PEP 8`_ compliant code
* Accurately determines relationships, including many-to-many, one-to-one
* Automatically detects joined table inheritance
* Excellent test coverage

.. _PEP 8: http://www.python.org/dev/peps/pep-0008/


Usage instructions
==================

Installation
------------

To install, do::

    pip install sqlacodegen


Example usage
-------------

At the minimum, you have to give sqlacodegen a database URL. The URL is passed directly to
SQLAlchemy's `create_engine()`_ method so please refer to `SQLAlchemy's documentation`_ for
instructions on how to construct a proper URL.

Examples::

    sqlacodegen postgresql:///some_local_db
    sqlacodegen mysql+oursql://user:password@localhost/dbname
    sqlacodegen sqlite:///database.db

To see the full list of options::

    sqlacodegen --help


.. _create_engine(): http://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.create_engine
.. _SQLAlchemy's documentation: http://docs.sqlalchemy.org/en/latest/core/engines.html


Why does it sometimes generate classes and sometimes Tables?
------------------------------------------------------------

Unless the ``--noclasses`` option is used, sqlacodegen tries to generate declarative model classes
from each table. There are two circumstances in which a ``Table`` is generated instead:

* the table has no primary key constraint (which is required by SQLAlchemy for every model class)
* the table is an association table between two other tables (see below for the specifics)


Model class naming logic
------------------------

The table name (which is assumed to be in English) is converted to singular form using the
"inflect" library. Then, every underscore is removed while transforming the next letter to upper
case. For example, ``sales_invoices`` becomes ``SalesInvoice``.


Relationship detection logic
----------------------------

Relationships are detected based on existing foreign key constraints as follows:

* **many-to-one**: a foreign key constraint exists on the table
* **one-to-one**: same as **many-to-one**, but a unique constraint exists on the column(s) involved
* **many-to-many**: an association table is found to exist between two tables

A table is considered an association table if it satisfies all of the following conditions:

#. has exactly two foreign key constraints
#. all its columns are involved in said constraints


Relationship naming logic
-------------------------

Relationships are typically named based on the opposite class name. For example, if an ``Employee``
class has a column named ``employer`` which has a foreign key to ``Company.id``, the relationship
is named ``company``.

A special case for single column many-to-one and one-to-one relationships, however, is if the
column is named like ``employer_id``. Then the relationship is named ``employer`` due to that
``_id`` suffix.

If more than one relationship would be created with the same name, the latter ones are appended
numeric suffixes, starting from 1.


Getting help
============

If you have problems or other questions, you can either:

* Ask on the `SQLAlchemy Google group`_, or
* Ask on the ``#sqlalchemy`` channel on `Freenode IRC`_

.. _SQLAlchemy Google group: http://groups.google.com/group/sqlalchemy
.. _Freenode IRC: http://freenode.net/irc_servers.shtml