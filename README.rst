.. image:: https://github.com/agronholm/sqlacodegen/actions/workflows/test.yml/badge.svg
  :target: https://github.com/agronholm/sqlacodegen/actions/workflows/test.yml
  :alt: Build Status
.. image:: https://coveralls.io/repos/github/agronholm/sqlacodegen/badge.svg?branch=master
  :target: https://coveralls.io/github/agronholm/sqlacodegen?branch=master
  :alt: Code Coverage

This is a tool that reads the structure of an existing database and generates the
appropriate SQLAlchemy model code, using the declarative style if possible.

This tool was written as a replacement for `sqlautocode`_, which was suffering from
several issues (including, but not limited to, incompatibility with Python 3 and the
latest SQLAlchemy version).

.. _sqlautocode: http://code.google.com/p/sqlautocode/


Features
========

* Supports SQLAlchemy 1.4.x
* Produces declarative code that almost looks like it was hand written
* Produces `PEP 8`_ compliant code
* Accurately determines relationships, including many-to-many, one-to-one
* Automatically detects joined table inheritance
* Excellent test coverage

.. _PEP 8: http://www.python.org/dev/peps/pep-0008/


Installation
============

To install, do::

    pip install sqlacodegen

To include support for the PostgreSQL ``CITEXT`` extension type (which should be
considered as tested only under a few environments) specify the ``citext`` extra::

    pip install sqlacodegen[citext]


Quickstart
==========

At the minimum, you have to give sqlacodegen a database URL. The URL is passed directly
to SQLAlchemy's `create_engine()`_ method so please refer to
`SQLAlchemy's documentation`_ for instructions on how to construct a proper URL.

Examples::

    sqlacodegen postgresql:///some_local_db
    sqlacodegen --generator tables mysql+pymysql://user:password@localhost/dbname
    sqlacodegen --generator dataclasses sqlite:///database.db

To see the list of generic options::

    sqlacodegen --help

.. _create_engine(): http://docs.sqlalchemy.org/en/latest/core/engines.html#sqlalchemy.create_engine
.. _SQLAlchemy's documentation: http://docs.sqlalchemy.org/en/latest/core/engines.html

Available generators
====================

The selection of a generator determines the

The following built-in generators are available:

* ``tables`` (only generates ``Table`` objects, for those who don't want to use the ORM)
* ``declarative`` (the default; generates classes inheriting from ``declarative_base()``
* ``dataclasses`` (generates dataclass-based models; v1.4+ only)
* ``sqlmodels`` (generates model classes for SQLModel_)

.. _SQLModel: https://sqlmodel.tiangolo.com/

Generator-specific options
==========================

The following options can be turned on by passing them using ``--option`` (can be used
multiple times):

* ``tables``

  * ``noconstraints``: ignore constraints (foreign key, unique etc.)
  * ``nocomments``: ignore table/column comments
  * ``noindexes``: ignore indexes

* ``declarative``

  * all the options from ``tables``
  * ``use_inflect``: use the ``inflect`` library when naming classes and relationships
    (turning plural names into singular; see below for details)
  * ``nojoined``: don't try to detect joined-class inheritance (see below for details)
  * ``nobidi``: generate relationships in a unidirectional fashion, so only the
    many-to-one or first side of many-to-many relationships gets a relationship
    attribute, as on v2.X

* ``dataclasses``

  * all the options from ``declarative``

* ``sqlmodel``

  * all the options from ``declarative``

Model class generators
----------------------

The code generators that generate classes try to generate model classes whenever
possible. There are two circumstances in which a ``Table`` is generated instead:

* the table has no primary key constraint (which is required by SQLAlchemy for every
  model class)
* the table is an association table between two other tables (see below for the
  specifics)

Model class naming logic
++++++++++++++++++++++++

By default, table names are converted to valid PEP 8 compliant class names by replacing
all characters unsuitable for Python identifiers with ``_``. Then, each valid parts
(separated by underscores) are title cased and then joined together, eliminating the
underscores. So, ``example_name`` becomes ``ExampleName``.

If the ``use_inflect`` option is used, the table name (which is assumed to be in
English) is converted to singular form using the "inflect" library. For example,
``sales_invoices`` becomes ``SalesInvoice``. Since table names are not always in
English, and the inflection process is far from perfect, inflection is disabled by
default.

Relationship detection logic
++++++++++++++++++++++++++++

Relationships are detected based on existing foreign key constraints as follows:

* **many-to-one**: a foreign key constraint exists on the table
* **one-to-one**: same as **many-to-one**, but a unique constraint exists on the
  column(s) involved
* **many-to-many**: (not implemented on the ``sqlmodel`` generator) an association table
  is found to exist between two tables

A table is considered an association table if it satisfies all of the following
conditions:

#. has exactly two foreign key constraints
#. all its columns are involved in said constraints

Relationship naming logic
+++++++++++++++++++++++++

Relationships are typically named based on the table name of the opposite class.
For example, if a class has a relationship to another class with the table named
``companies``, the relationship would be named ``companies`` (unless the ``use_inflect``
option was enabled, in which case it would be named ``company`` in the case of a
many-to-one or one-to-one relationship).

A special case for single column many-to-one and one-to-one relationships, however, is
if the column is named like ``employer_id``. Then the relationship is named ``employer``
due to that ``_id`` suffix.

For self referential relationships, the reverse side of the relationship will be named
with the ``_reverse`` suffix appended to it.

Customizing code generation logic
=================================

If the built-in generators with all their options don't quite do what you want, you can
customize the logic by subclassing one of the existing code generator classes. Override
whichever methods you need, and then add an `entry point`_ in the
``sqlacodegen.generators`` namespace that points to your new class. Once the entry point
is in place (you typically have to install the project with ``pip install``), you can
use ``--generator <yourentrypoint>`` to invoke your custom code generator.

For examples, you can look at sqlacodegen's own entry points in its `pyproject.toml`_.

.. _entry point: https://setuptools.readthedocs.io/en/latest/userguide/entry_point.html
.. _pyproject.toml: https://github.com/agronholm/sqlacodegen/blob/master/pyproject.toml

Getting help
============

If you have problems or other questions, you can either:

* Ask on the `SQLAlchemy Google group`_, or
* Ask on the sqlalchemy_ room on Gitter

.. _SQLAlchemy Google group: http://groups.google.com/group/sqlalchemy
.. _sqlalchemy: https://gitter.im/sqlalchemy/community
