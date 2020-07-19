#!/usr/bin/env python

from setuptools import setup

setup(name='alpha-compiler-vk',
      version='0.1.3',
      description='Python tools for quantitative finance. [VK branch]',
      author='Peter Harrington',
      author_email='peter.b.harrington@gmail.com',
      maintainer='Vyacheslav Klyuchnikov',
      url='https://github.com/vk643/alpha-compiler-vk/',
      packages=['alphacompiler',
          'alphacompiler.util', 
          'alphacompiler.data', 
          'alphacompiler.data.loaders'], requires=['pandas', 'zipline', 'numpy']
      )
