Version history
===============

1.1.0
-----

* Revamped the API

* Added automatic detection of joined-table inheritance

* Fixed missing class name prefix in primary/secondary joins in relationships

* Instead of wildcard imports, generate explicit imports dynamically (fixes #1)

* Automatically detect Boolean columns based on CheckConstraints

* Skip redundant CheckConstraints for Enum and Boolean columns


1.0.0
-----

* Initial release
