""" Migrate Grafana dashboards from one Grafana instance to another.

This script is designed to be executed inside a github action.
It has three modes of operation: export, import and search.
"""
import argparse
import json
import os
import sys

import requests
from handler import GrafanaAPIHandler, GrafanaEnvironment


def __write_to_file(path: str, content: str) -> None:
    """ Write content to a file. """
    with open(path, 'w', encoding='utf-8') as fhandler:
        fhandler.write(content)


def __read_from_file(path: str) -> dict:
    """ Read content from a file and parse as json """
    with open(path, 'r', encoding='utf-8') as fhandler:
        return json.load(fhandler)


def parse_args(args):
    """ Parse the command line arguments. """
    parser = argparse.ArgumentParser(description='Migrate Grafana dashboards')
    parser.add_argument('mode', choices=['export', 'import'],
                        help='The mode of operation.')
    parser.add_argument('--file', default='.',
                        help='The file to the export JSON file.')
    parser.add_argument('--api-key', help='The Grafana API key.')
    parser.add_argument(
        '--base-url', help='The base URL of the Grafana instance.')
    parser.add_argument('--folder', help="Grafana folder")
    parser.add_argument('--dashboard', help="Grafana dashboard name")
    parser.add_argument('--environment', help="Import Environment level name")
    return parser.parse_args(args)


def __prepare_dump_before_import(export_dump: dict, target_env: str, new_datasource_uid: str):
    """ Edits export dump before import. """
    print("::group::export_dump")
    print(export_dump)
    print("::endgroup::")

    # setting dashboard id and uid to None to allow grafana to create a new dashboard
    export_dump['id'] = None
    export_dump['uid'] = None

    # replace environment prefix in collection and datasource
    panels = export_dump['panels']
    for panel in panels:
        if 'datasource' in panel:
            panel['datasource']['uid'] = new_datasource_uid
        for target in panel['targets'] if 'targets' in panel else []:
            collection = target['collection']
            coll_split = collection.split('-')
            target['collection'] = f"{target_env}-{'-'.join(coll_split[1:])}"
            if 'datasource' in target:
                target['datasource']['uid'] = new_datasource_uid

    return export_dump


def main():
    """ Main function. """
    gf_base_url = os.environ.get('GF_BASE_URL', 'http://localhost:3000')
    gf_auth_token = os.environ.get('GF_AUTH_TOKEN', None)

    args = parse_args(sys.argv[1:])

    gf_env = GrafanaEnvironment(
        base_url=args.base_url if args.base_url else gf_base_url,
        auth_token=args.api_key if args.api_key else gf_auth_token
    )
    handler = GrafanaAPIHandler(gf_env, requests)

    if args.mode == 'export':
        export_dump = export_dashboard(handler,
                                       args.folder, args.dashboard)
        __write_to_file(args.file, json.dumps(export_dump))
    elif args.mode == 'import':
        export_dump = __read_from_file(args.file)
        new_datasource_uid = handler.get_datasource_uid()
        prepared_export_dump = __prepare_dump_before_import(
            export_dump, args.environment, new_datasource_uid)
        new_folder_id = get_or_create_folder(handler, args.folder)
        handler.import_dashboard(prepared_export_dump, new_folder_id)
    else:
        raise ValueError(f'Invalid mode of operation: {args.mode}')


def export_dashboard(handler: GrafanaAPIHandler, folder_name: str, dashboard_name: str) -> dict:
    """ Export selected dashboard to dump file """
    dashboards = handler.list_dashboards()

    for dash in dashboards:
        if 'folderTitle' not in dash:
            dash['folderTitle'] = 'General'

    filtered_dashboards_uids = [
        dash['uid']
        for dash in dashboards
        if dash['title'] == dashboard_name
        if dash['folderTitle'] == folder_name
    ]

    if len(filtered_dashboards_uids) == 0:
        raise ValueError(
            f'Unable to find dashboard with title: {dashboard_name}')

    return handler.export_dashboard(filtered_dashboards_uids[0])


def get_or_create_folder(handler: GrafanaAPIHandler, folder_name: str) -> int:
    """ Gets or creates a folder in Grafana and returns its id. """
    folder_id = handler.get_folder_id(folder_name)
    if folder_id is None:
        folder_id = handler.create_folder(folder_name)
    return folder_id


if __name__ == '__main__':
    main()
