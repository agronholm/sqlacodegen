import sys
import os.path

from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand

import sqlacodegen


class PyTest(TestCommand):
    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        import pytest
        errno = pytest.main(self.test_args)
        sys.exit(errno)


extra_requirements = ()
if sys.version_info < (2, 7):
    extra_requirements = ('argparse',)

here = os.path.dirname(__file__)
readme_path = os.path.join(here, 'README.rst')
readme = open(readme_path).read()

setup(
    name='sqlacodegen',
    description='Automatic model code generator for SQLAlchemy',
    long_description=readme,
    version=sqlacodegen.__version__,
    author='Alex Gronholm',
    author_email='sqlacodegen@nextday.fi',
    url='http://pypi.python.org/pypi/sqlacodegen/',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
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
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4'
    ],
    keywords='sqlalchemy',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    install_requires=(
        'SQLAlchemy >= 0.6.0',
        'inflect >= 0.2.0'
    ) + extra_requirements,
    tests_require=['pytest', 'pytest-pep8'],
    cmdclass={'test': PyTest},
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'sqlacodegen=sqlacodegen.main:main'
        ]
    }
)
