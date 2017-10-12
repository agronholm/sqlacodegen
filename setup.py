import os.path

from setuptools import setup, find_packages


here = os.path.dirname(__file__)
readme_path = os.path.join(here, 'README.rst')
readme = open(readme_path).read()

setup(
    name='sqlacodegen',
    description='Automatic model code generator for SQLAlchemy',
    long_description=readme,
    use_scm_version={
        'version_scheme': 'post-release',
        'local_scheme': 'dirty-tag'
    },
    author='Alex Gronholm',
    author_email='sqlacodegen@nextday.fi',
    url='https://github.com/agronholm/sqlacodegen',
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
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5'
    ],
    keywords='sqlalchemy',
    license='MIT',
    packages=find_packages(exclude=['tests']),
    setup_requires=[
        'setuptools_scm >= 1.7.0'
    ],
    install_requires=(
        'SQLAlchemy >= 0.6.0',
        'inflect >= 0.2.0'
    ),
    extras_require={
        ':python_version == "2.6"': ['argparse']
    },
    zip_safe=False,
    entry_points={
        'console_scripts': [
            'sqlacodegen = sqlacodegen.main:main'
        ]
    }
)
