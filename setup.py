#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re

from setuptools import setup

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()


def load_reqs(filename):
    with open(filename) as reqs_file:
        return [
            re.sub('==', '>=', line) for line in reqs_file.readlines()
            if not re.match(r'(\s*#|-r)', line)
        ]


requirements = load_reqs('requirements/base.txt')
test_requirements = load_reqs('requirements/test.txt')

setup(
    name='etcd3ref',
    version='0.1.0',
    description="Python client for the etcd v3 API",
    long_description=readme + '\n\n' + history,
    author="Louis Taylor",
    author_email='louis@kragniz.eu',
    url='https://github.com/Hochikong/python-etcd3',
    packages=[
        'etcd3ref',
        'etcd3ref.etcdrpc',
    ],
    package_dir={
        'etcd3ref': 'etcd3ref',
        'etcd3ref.etcdrpc': 'etcd3ref/etcdrpc',
    },
    include_package_data=True,
    install_requires=requirements,
    license="Apache Software License 2.0",
    zip_safe=False,
    keywords='etcd3ref',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
