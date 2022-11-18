import os
import subprocess
import json
from pathlib import Path
from typing import Set


def summary(text):
    print(text)
    os.environ['GITHUB_STEP_SUMMARY'] = os.environ.get('GITHUB_STEP_SUMMARY', '') + text


def add_all_dependant_directories(changed_directories: Set[str], requirement: str) -> Set[str]:
    """
    iterates over directories searching for requirements files that contains a given requirement string
    """
    for file in os.scandir(os.getcwd()):
        if file.is_dir():
            requirements_file = Path(os.path.join(file.path, "requirements.txt"))
            if requirements_file.is_file():
                with open(requirements_file) as requirements_content:
                    # If requirement is referenced in a service's requirements.txt it depends on it
                    if f"-e ../{requirement}" in requirements_content.read():
                        changed_directories.add(file.name)
    return changed_directories


def determine_changed_directories():
    # Get all changed directories
    process = subprocess.run(f"git diff --name-only {os.environ.get('GITHUB_EVENT_BEFORE')}..{ os.environ.get('GITHUB_EVENT_AFTER') }", capture_output=True, shell=True)

    changes = []
    if process.returncode == 0:
        changes = process.stdout.decode('ascii').splitlines()
    else:
        summary(f"Something went wrong when running git diff: '{process.stderr.decode('ascii')}'")
        exit(1)

    changed_directories = set(map(lambda x: Path(x).parts[0], changes))

    summary("### Build summary")

    if "base" in changed_directories:
        summary("Base was changed, therefore all dependent services will be built!")
        changed_directories = add_all_dependant_directories(changed_directories, "base")
    if "basehandler" in changed_directories:
        summary("basehandler was changed, building ivschain services")
        changed_directories = add_all_dependant_directories(changed_directories, "basehandler")

    changed_directories = set(changed_directories)
    # Remove directories which don't exist as git diff also lists deleted files
    existing_dirs = filter(lambda f: Path(f).is_dir(), changed_directories)

    # Filter out non-service directories
    return existing_dirs


changed_directories = []
if os.environ.get('GITHUB_EVENT_NAME') == 'workflow_dispatch':
    if os.environ.get('INPUT_SERVICES'):
        input_services = os.environ.get('INPUT_SERVICES', '').split(',')
        input_services = list(map(lambda name: Path(name.strip()), input_services))
    else:
        input_services = os.scandir(os.getcwd())
    changed_directories = list(map(lambda file: file.name, filter(lambda file: file.is_dir(), input_services)))
    summary(f"Manual run triggered therefore selected services will be built.")
elif os.environ.get('GITHUB_EVENT_NAME') == 'push':
    summary("Automated run therefore only changed services will be built!")
    changed_directories = determine_changed_directories()

changed_services = list(filter(lambda f: f not in json.loads(os.environ['EXCLUDED_DIRS']), changed_directories))

summary(f"The following services will be built: {changed_services}")
print(f"::set-output name=services::{json.dumps(changed_services)}")
