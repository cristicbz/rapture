#!/usr/bin/env python
# -*- coding: utf-8 -*-
from setuptools import setup


setup(name='rapture',
      version='0.2.0',
      description='Rapture job queue.',
      author='reinfer.io Ltd.',
      author_email='marius@reinfer.io',
      url='https://github.com/reinfer/rapture',
      test_suite='test',
      install_requires=[
          'redis>=2.9.1',
      ],
      packages=['rapture']
)
