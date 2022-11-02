""" Module that should be use dby all ivs chain handlers. """
import os

from setuptools import setup

current_directory = os.path.dirname(os.path.abspath(__file__))
install_requires = [
    "backoff",
    "requests",
    "flask",
    "elastic-apm",
    "base[aws]",
]

setup(
    name="basehandler",
    version="3.0.0",
    packages=["basehandler"],
    install_requires=install_requires,
    dependency_links=[
        os.path.abspath(os.path.join(current_directory, "..", "base"))
    ]
)
