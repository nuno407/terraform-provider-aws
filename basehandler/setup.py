from setuptools import setup
import os

current_directory = os.path.dirname(os.path.abspath(__file__))
install_requires =[
        'backoff',
        'requests',
        'flask',
        'elastic-apm',
        "base[aws]==3.0.0",
    ]

setup(
    name='basehandler',
    version='3.0.0',
    packages=['basehandler'],
    install_requires=install_requires,
    dependency_links=[
        os.path.abspath(os.path.join(current_directory, '..', 'base'))
    ]
)
