Version history
===============

**UNRELEASED**

- **BACKWARD INCOMPATIBLE** Relationship names changed when multiple FKs or junction tables
  connect to the same target table. Regenerating models will break existing code.
- Improved relationship naming: one-to-many uses FK column names (e.g.,
  ``simple_items_parent_container``), many-to-many uses junction table names (e.g.,
  ``students_enrollments``). Use ``--options nofknames`` to revert to old behavior.

**4.0.0rc2**

- Add ``values_callable`` lambda to generated native enums column definitions.
  This allows for proper enum value insertion when working with ORM models (PR by @sheinbergon)

**4.0.0rc1**

- **BACKWARD INCOMPATIBLE** ``TablesGenerator.render_column_type()`` was changed to
  receive the ``Column`` object instead of the column type object as its sole argument
- Added Python enum generation for native database ENUM types (e.g., PostgreSQL / MySQL ENUM).
  Retained synthetic Python enum generation from CHECK constraints with
  IN clauses (e.g., ``column IN ('val1', 'val2', ...)``). Use ``--options nonativeenums`` to
  disable enum generation for native database enums. Use ``--options nosyntheticenums`` to
  disable enum generation for synthetic database enums (VARCHAR columns with check constraints).
  (PR by @sheinbergon)

**3.2.0**

- Dropped support for Python 3.9
- Fix Postgres ``DOMAIN`` adaptation regression introduced in SQLAlchemy 2.0.42 (PR by @sheinbergon)
- Support disabling special naming logic for single column many-to-one and one-to-one relationships
  (PR by @Henkhogan, revised by @sheinbergon)
- Add ``include_dialect_options`` option to render ``Table`` and ``Column``
  dialect-specific kwargs and ``info`` in generated code. (PR by @jaogoy)
- Add ``keep_dialect_types`` option to preserve dialect-specific column types instead of
  adapting to generic SQLAlchemy types. (PR by @jaogoy)

**3.1.1**

- Fallback ``NotImplemented`` errors encountered when accessing ``python_type`` for
  non-native types to ``typing.Any``
  (PR by @sheinbergon, based on work by @danplischke)

**3.1.0**

- Type annotations for ARRAY column attributes now include the Python type of
  the array elements
- Added support for specifying engine arguments via ``--engine-arg``
  (PR by @LajosCseppento)
- Fixed incorrect package name used in ``importlib.metadata.version`` for
  ``sqlalchemy-citext``, resolving ``PackageNotFoundError`` (PR by @oaimtiaz)
- Prevent double pluralization (PR by @dkratzert)
- Fixes DOMAIN extending JSON/JSONB data types (PR by @sheinbergon)
- Temporarily restrict SQLAlchemy version to 2.0.41 (PR by @sheinbergon)
- Fixes ``add_import`` behavior when adding imports from sqlalchemy and overall better
  alignment of import behavior(s) across generators
- Fixes ``nullable`` column behavior for non-null columns for both
  ``sqlmodels`` and ``declarative`` generators (PR by @sheinbergon)

**3.0.0**

- Dropped support for Python 3.8
- Changed nullable relationships to include ``Optional`` in their type annotations
- Fixed SQLModel code generation
- Fixed two rendering issues in ``ENUM`` columns when a non-default schema is used: an
  unwarranted positional argument and missing the ``schema`` argument
- Fixed ``AttributeError`` when metadata contains user defined column types
- Fixed ``AssertionError`` when metadata contains a column type that is a type decorator
  with an all-uppercase name
- Fixed MySQL ``DOUBLE`` column types being rendered with the wrong arguments

**3.0.0rc5**

- Fixed pgvector support not working

**3.0.0rc4**

- Dropped support for Python 3.7
- Dropped support for SQLAlchemy 1.x
- Added support for the ``pgvector`` extension (with help from KellyRousselHoomano)

**3.0.0rc3**

- Added support for SQLAlchemy 2 (PR by rbuffat with help from mhauru)
- Renamed ``--option`` to ``--options`` and made its values delimited by commas
- Restored CIText and GeoAlchemy2 support (PR by stavvy-rotte)

**3.0.0rc2**

- Added support for generating SQLModel classes (PR by Andrii Khirilov)
- Fixed code generation when a single-column index is unique or does not match the
  dialect's naming convention (PR by Leonardus Chen)
- Fixed another problem where sequence schemas were not properly separated from the
  sequence name
- Fixed invalid generated primary/secondaryjoin expressions in self-referential
  many-to-many relationships by using lambdas instead of strings
- Fixed ``AttributeError`` when the declarative generator encounters a table name
  already in singular form when ``--option use_inflect`` is enabled
- Increased minimum SQLAlchemy version to 1.4.36 to address issues with ``ForeignKey``
  and indexes, and to eliminate the PostgreSQL UUID column type annotation hack

**3.0.0rc1**

- Migrated all packaging/testing configuration to ``pyproject.toml``
- Fixed unwarranted ``ForeignKey`` declarations appearing in column attributes when
  there are named, single column foreign key constraints (PR by Leonardus Chen)
. Fixed ``KeyError`` when rendering an index without any columns
- Fixed improper handling of schema prefixes in sequence names in server defaults
- Fixed identically named tables from different schemas resulting in invalid generated
  code
- Fixed imports caused by ``server_default`` conflicting with class attribute names
- Worked around PostgreSQL UUID columns getting ``Any`` as the type annotation

**3.0.0b3**

- Dropped support for Python < 3.7
- Dropped support for SQLAlchemy 1.3
- Added a ``__main__`` module which can be used as an alternate entry point to the CLI
- Added detection for sequence use in column defaults on PostgreSQL
- Fixed ``sqlalchemy.exc.InvalidRequestError`` when encountering a column named
  "metadata" (regression from 2.0)
- Fixed missing ``MetaData`` import with ``DeclarativeGenerator`` when only plain tables
  are generated
- Fixed invalid data classes being generated due to some relationships having been
  rendered without a default value
- Improved translation of column names into column attributes where the column name has
  whitespace at the beginning or end
- Modified constraint and index rendering to add them explicitly instead of using
  shortcuts like ``unique=True``, ``index=True`` or ``primary=True`` when the constraint
  or index has a name that does not match the default naming convention

**3.0.0b2**

- Fixed ``IDENTITY`` columns not rendering properly when they are part of the primary
  key

**3.0.0b1**

**NOTE**: Both the API and the command line interface have been refactored in a
backwards incompatible fashion. Notably several command line options have been moved to
specific generators and are no longer visible from ``sqlacodegen --help``. Their
replacement are documented in the README.

- Dropped support for Python < 3.6
- Added support for Python 3.10
- Added support for SQLAlchemy 1.4
- Added support for bidirectional relationships (use ``--option nobidi``) to disable
- Added support for multiple schemas via ``--schemas``
- Added support for ``IDENTITY`` columns
- Disabled inflection during table/relationship name generation by default
  (use ``--option use_inflect`` to re-enable)
- Refactored the old ``CodeGenerator`` class into separate generator classes, selectable
  via ``--generator``
- Refactored several command line options into generator specific options:

  - ``--noindexes`` → ``--option noindexes``
  - ``--noconstraints`` → ``--option noconstraints``
  - ``--nocomments`` → ``--option nocomments``
  - ``--nojoined`` → ``--option nojoined`` (``declarative`` and ``dataclass`` generators
    only)
  - ``--noinflect`` → (now the default; use ``--option use_inflect`` instead)
    (``declarative`` and ``dataclass`` generators only)
- Fixed missing import for ``JSONB`` ``astext_type`` argument
- Fixed generated column or relationship names colliding with imports or each other
- Fixed ``CompileError`` when encountering server defaults that contain colons (``:``)

**2.3.0**

- Added support for rendering computed columns
- Fixed ``--nocomments`` not taking effect (fix proposed by AzuresYang)
- Fixed handling of MySQL ``SET`` column types (and possibly others as well)

**2.2.0**

- Added support for rendering table comments (PR by David Hirschfeld)
- Fixed bad identifier names being generated for plain tables (PR by softwarepk)

**2.1.0**

- Dropped support for Python 3.4
- Dropped support for SQLAlchemy 0.8
- Added support for Python 3.7 and 3.8
- Added support for SQLAlchemy 1.3
- Added support for column comments (requires SQLAlchemy 1.2+; based on PR by koalas8)
- Fixed crash on unknown column types (``NullType``)

**2.0.1**

- Don't adapt dialect specific column types if they need special constructor arguments
  (thanks Nicholas Martin for the PR)

**2.0.0**

- Refactored code for better reuse
- Dropped support for Python 2.6, 3.2 and 3.3
- Dropped support for SQLAlchemy < 0.8
- Worked around a bug regarding Enum on SQLAlchemy 1.2+ (``name`` was missing)
- Added support for Geoalchemy2
- Fixed invalid class names being generated (fixes #60; PR by Dan O'Huiginn)
- Fixed array item types not being adapted or imported
  (fixes #46; thanks to Martin Glauer and Shawn Koschik for help)
- Fixed attribute name of columns named ``metadata`` in mapped classes (fixes #62)
- Fixed rendered column types being changed from the original (fixes #11)
- Fixed server defaults which contain double quotes (fixes #7, #17, #28, #33, #36)
- Fixed ``secondary=`` not taking into account the association table's schema name
  (fixes #30)
- Sort models by foreign key dependencies instead of schema and name (fixes #15, #16)

**1.1.6**

- Fixed compatibility with SQLAlchemy 1.0
- Added an option to only generate tables

**1.1.5**

- Fixed potential assignment of columns or relationships into invalid attribute names
  (fixes #10)
- Fixed unique=True missing from unique Index declarations
- Fixed several issues with server defaults
- Fixed potential assignment of columns or relationships into invalid attribute names
- Allowed pascal case for tables already using it
- Switched from Mercurial to Git

**1.1.4**

- Fixed compatibility with SQLAlchemy 0.9.0

**1.1.3**

- Fixed compatibility with SQLAlchemy 0.8.3+
- Migrated tests from nose to pytest

**1.1.2**

- Fixed non-default schema name not being present in __table_args__ (fixes #2)
- Fixed self referential foreign key causing column type to not be rendered
- Fixed missing "deferrable" and "initially" keyword arguments in ForeignKey constructs
- Fixed foreign key and check constraint handling with alternate schemas (fixes #3)

**1.1.1**

- Fixed TypeError when inflect could not determine the singular name of a table for a
  many-to-1 relationship
- Fixed _IntegerType, _StringType etc. being rendered instead of proper types on MySQL

**1.1.0**

- Added automatic detection of joined-table inheritance
- Fixed missing class name prefix in primary/secondary joins in relationships
- Instead of wildcard imports, generate explicit imports dynamically (fixes #1)
- Use the inflect library to produce better guesses for table to class name conversion
- Automatically detect Boolean columns based on CheckConstraints
- Skip redundant CheckConstraints for Enum and Boolean columns

**1.0.0**

- Initial release
