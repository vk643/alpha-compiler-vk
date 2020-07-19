#!/usr/bin/env python

from distutils.core import setup

setup(name='alpha-compiler-vk',
      version='0.1a',
      description='Python tools for quantitative finance',
      author='Peter Harrington',
      author_email='peter.b.harrington@gmail.com',
      maintainer='Vyacheslav Klyuchnikov',
      url='https://www.alphacompiler.com/',
      packages=['alphacompiler',
          'alphacompiler.util', 
          'alphacompiler.data', 
          'alphacompiler.data.loaders'], requires=['pandas', 'zipline', 'numpy']
      )
