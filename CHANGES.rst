Version history
===============

2.2.0
-----

* Added support for rendering table comments (PR by David Hirschfeld)
* Fixed bad identifier names being generated for plain tables (PR by softwarepk)


2.1.0
-----

* Dropped support for Python 3.4
* Dropped support for SQLAlchemy 0.8
* Added support for Python 3.7 and 3.8
* Added support for SQLAlchemy 1.3
* Added support for column comments (requires SQLAlchemy 1.2+; based on PR by koalas8)
* Fixed crash on unknown column types (``NullType``)


2.0.1
-----

* Don't adapt dialect specific column types if they need special constructor arguments
  (thanks Nicholas Martin for the PR)


2.0.0
-----

* Refactored code for better reuse

* Dropped support for Python 2.6, 3.2 and 3.3

* Dropped support for SQLAlchemy < 0.8

* Worked around a bug regarding Enum on SQLAlchemy 1.2+ (``name`` was missing)

* Added support for Geoalchemy2

* Fixed invalid class names being generated (fixes #60; PR by Dan O'Huiginn)

* Fixed array item types not being adapted or imported
  (fixes #46; thanks to Martin Glauer and Shawn Koschik for help)

* Fixed attribute name of columns named ``metadata`` in mapped classes (fixes #62)

* Fixed rendered column types being changed from the original (fixes #11)

* Fixed server defaults which contain double quotes (fixes #7, #17, #28, #33, #36)

* Fixed ``secondary=`` not taking into account the association table's schema name (fixes #30)

* Sort models by foreign key dependencies instead of schema and name (fixes #15, #16)


1.1.6
-----

* Fixed compatibility with SQLAlchemy 1.0

* Added an option to only generate tables


1.1.5
-----

* Fixed potential assignment of columns or relationships into invalid attribute names (fixes #10)

* Fixed unique=True missing from unique Index declarations

* Fixed several issues with server defaults

* Fixed potential assignment of columns or relationships into invalid attribute names

* Allowed pascal case for tables already using it

* Switched from Mercurial to Git


1.1.4
-----

* Fixed compatibility with SQLAlchemy 0.9.0


1.1.3
-----

* Fixed compatibility with SQLAlchemy 0.8.3+

* Migrated tests from nose to pytest


1.1.2
-----

* Fixed non-default schema name not being present in __table_args__ (fixes #2)

* Fixed self referential foreign key causing column type to not be rendered

* Fixed missing "deferrable" and "initially" keyword arguments in ForeignKey constructs

* Fixed foreign key and check constraint handling with alternate schemas (fixes #3)


1.1.1
-----

* Fixed TypeError when inflect could not determine the singular name of a table for a many-to-1 relationship

* Fixed _IntegerType, _StringType etc. being rendered instead of proper types on MySQL


1.1.0
-----

* Added automatic detection of joined-table inheritance

* Fixed missing class name prefix in primary/secondary joins in relationships

* Instead of wildcard imports, generate explicit imports dynamically (fixes #1)

* Use the inflect library to produce better guesses for table to class name conversion

* Automatically detect Boolean columns based on CheckConstraints

* Skip redundant CheckConstraints for Enum and Boolean columns


1.0.0
-----

* Initial release
