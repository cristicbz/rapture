#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup


setup(name='reinferio.jobs',
      version='0.1.0',
      description='reinfer.io web and related Python library',
      author='reinfer.io Ltd.',
      author_email='marius@reinfer.io',
      url='https://github.com/reinferio/rapture',
      test_suite='test',
      install_requires=[
          'redis>=2.9.1',
      ],
      packages=[
          'reinferio',
          'reinferio.jobs',
      ],
      namespace_packages=[
          'reinferio'
      ]
     )
