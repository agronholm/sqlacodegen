Version history
===============

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
