import sys

from setuptools import setup, find_packages


extra_requirements = ('argparse',) if sys.version_info < (2, 7) else ()

setup(
    name='sqlcodegen',
    description='Automatic model code generator for SQLAlchemy',
    version='1.0.0.pre1',
    author='Alex Gronholm',
    author_email='sqlcodegen@nextday.fi',
    url='http://pypi.python.org/pypi/sqlcodegen/',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Environment :: Console',
        'Topic :: Database',
        'Topic :: Software Development :: Code Generators',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.2',
        'Programming Language :: Python :: 3.3'
    ],
    packages=find_packages(exclude=['tests']),
    install_requires=(
        'sqlalchemy >= 0.6.0',
    ) + extra_requirements,
    test_suite='nose.collector',
    tests_require=['nose'],
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'sqlcodegen=sqlcodegen.main:main'
        ]
    }
)
