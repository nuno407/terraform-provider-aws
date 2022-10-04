import importlib
import pkgutil


def calculate_all_submodules(package):
    results = []
    for _, name, is_package in pkgutil.walk_packages(path=package.__path__):
        full_name = package.__name__ + '.' + name
        results.append(full_name)
        if is_package:
            results.extend(calculate_all_submodules(importlib.import_module(full_name)))
    return results


def test_import_of_all_submodules():
    # Objective of this test is to test if the dispendencies installed via pip install .[all] are correct
    import base
    for module_name in calculate_all_submodules(base):
        # voxel has some dependencies with pandas that can not be solved.
        if 'voxel' not in module_name:
            __import__(module_name)
