from setuptools import setup

setup(name='baseaws',
      version='3.0',
      # list folders, not files
      packages=['baseaws'],
      install_requires=['boto3', 'expiringdict']
      )
