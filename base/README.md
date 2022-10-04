# Base Library

The objective of this library maintain in a single place all common code from our services.


## How to use

This library contains multiple modules inside:

* base - contains generic code. To use you should do `pip install -e .`
* aws - contains helper functions for AWS operations. To use you should do `pip install -e  .[aws]`
* testing - contains helper functions for testing. To use you should do `pip install -e  .[testing]`
* voxel - contains helper functions for voxel operations. To use you should do `pip install -e  .[voxel]`

If you want to use all modules please do  `pip install -e .[all]` or, for example, `pip install -e .[aws,voxel]` for a selected part of the modules.

## How to test

Install the library as `pip install -r requirements_dev.txt` and do `pytest` inside tests folder
