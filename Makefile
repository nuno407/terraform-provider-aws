PYTHON_SERVICES_DIRS = anon_ivschain base basehandler chc_ivschain MDFParser Metadata SDM SDRetriever Selector
PYENV_ROOT=${HOME}/.pyenv
TOKEN=${VOXEL_REGISTRY_TOKEN}
PYTHON_VERSION=3.9.12


setup-single-python-env:
	# Tries to create virtualenv. Ignores if already created
	pyenv virtualenv --force $(PYTHON_VERSION) $$python_service_dir
	# Creates a .venv link between pyenv instalation and service directory.
	# This is done for VSCode automatic discovery of python interperter
	-test -d $$python_service_dir/.venv && rm -R $$python_service_dir/.venv
	ln -s $(PYENV_ROOT)/versions/$$python_service_dir $$python_service_dir/.venv
	# Install of dependencies
	-test -f $$python_service_dir/requirements.txt && \
	cd $$python_service_dir && \
	TOKEN=$(TOKEN) $(PYENV_ROOT)/versions/$$python_service_dir/bin/python -m pip install -r requirements.txt
	# Install of dev dependencies
	-test -f $$python_service_dir/requirements_dev.txt && \
	cd $$python_service_dir && \
	TOKEN=$(TOKEN) $(PYENV_ROOT)/versions/$$python_service_dir/bin/python -m pip install -r requirements_dev.txt


setup-python-virtual-envs:
	# Try to install necessary version
	pyenv install $(PYTHON_VERSION) --skip-existing
	for python_service_dir in $(PYTHON_SERVICES_DIRS); do \
		$(MAKE) setup-single-python-env python_service_dir=$$python_service_dir; \
	done


remove-all-python-virtual-envs:
	for python_service_dir in $(PYTHON_SERVICES_DIRS); do \
		pyenv virtualenv-delete $$python_service_dir; \
	done
