__author__ = 'samantha'

from setuptools import setup

setup(name='zinterface',
      version='0.1',
      description='0MQ based RPC pubsub and PUSHPULL client and server abstractions',
      author='Samantha Atkins',
      author_email='sjatkins@gmail.com',
      license='BSD',
      packages=['zinterface'],
      install_requires=['pyzmq'],
      zip_safe=False)
