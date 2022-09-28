from setuptools import setup

setup(name='baseaws',
      version='3.0',
      # list folders, not files
      packages=['baseaws'],
      install_requires=['boto3', 'expiringdict',
                        'boto3-stubs[kinesis-video-archived-media]', 'boto3-stubs[s3]', 'boto3-stubs[sts]', 'boto3-stubs[kinesisvideo]'],
      )
