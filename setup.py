import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, 'README.rst')) as f:
    README = f.read()

requires = [
    'gevent',
    'setuptools',
    'couchdb',
    'pytz',
    'libnacl',
]
api_requires = [
    'openprocurement.api',
]
bridge_requires = requires + [
    'openprocurement_client',
    'openprocurement.edge',
    'boto',
]
test_requires = bridge_requires + api_requires + [
    'webtest',
    'python-coveralls',
    'nose',
    'mock',
    'munch'
]

entry_points = {
    'openprocurement.archivarius.storages': [
        's3 = openprocurement.archivarius.core.storages.storages:s3',
        'couchdb = openprocurement.archivarius.core.storages.storages:couch'
    ],
    'console_scripts': [
        'archivarius = openprocurement.archivarius.core.bridge:main'
    ]
}

setup(name='openprocurement.archivarius.core',
      version='1.0.1',
      description='openprocurement.archivarius.core',
      long_description=README,
      classifiers=[
          "Framework :: Pylons",
          "License :: OSI Approved :: Apache Software License",
          "Programming Language :: Python",
          "Topic :: Internet :: WWW/HTTP",
          "Topic :: Internet :: WWW/HTTP :: WSGI :: Application"
      ],
      keywords="web services",
      author='Quintagroup, Ltd.',
      author_email='info@quintagroup.com',
      license='Apache License 2.0',
      url='https://github.com/openprocurement/openprocurement.archivarius.core',
      packages=find_packages(exclude=['ez_setup']),
      namespace_packages=['openprocurement', 'openprocurement.archivarius'],
      include_package_data=True,
      zip_safe=False,
      install_requires=requires,
      tests_require=test_requires,
      extras_require={'bridge': bridge_requires, 'test': test_requires},
      test_suite="openprocurement.archivarius.core.tests.main.suite",
      entry_points=entry_points)
