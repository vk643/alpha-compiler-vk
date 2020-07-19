#!/usr/bin/env python

from setuptools import setup

setup(name='alpha-compiler-vk',
      version='0.1.1',
      description='Python tools for quantitative finance (originally by Peter Harrington, peter.b.harrington@gmail.com)',
      author='Vyacheslav Klyuchnikov',
      author_email='vyacheslav.klyuchnikov@gmail.com',
      url='https://github.com/vk643/alpha-compiler-vk/',
      packages=['alphacompiler',
          'alphacompiler.util', 
          'alphacompiler.data', 
          'alphacompiler.data.loaders'], requires=['pandas', 'zipline', 'numpy']
      )
