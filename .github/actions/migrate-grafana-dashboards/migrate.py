""" Migrate Grafana dashboards from one Grafana instance to another.

This script is designed to be executed inside a github action.
It has two modes of operation: export and import.
"""
import argparse
import os
from dataclasses import dataclass

import requests

GF_EXPORT_DASH_ENDPOINT = "/api/dashboards/uid/{}"
GF_IMPORT_DASH_ENDPOINT = "/api/dashboards/db"

REQUEST_TIMEOUT_IN_SECONDS = 5

@dataclass
class GrafanaEnvironment:
    """ Grafana environment variables. """
    base_url: str
    auth_token: str

def get_authorization_header(api_key: str) -> dict:
    """ Get the authorization header. """
    return {"Authorization": f"Bearer {api_key}"}


def write_to_file(path: str, content: str) -> None:
    """ Write content to a file. """
    with open(path, 'w', encoding='utf-8') as fhandler:
        fhandler.write(content)


def read_from_file(path: str) -> str:
    """ Read content from a file. """
    with open(path, 'r', encoding='utf-8') as fhandler:
        return fhandler.read()


def export_dashboard(gf_env: GrafanaEnvironment, uid: str, path: str) -> None:
    """ Export a dashboard from Grafana.

    Response example:
    {
        "dashboard": {
            "id": 1,
            "uid": "cIBgcSjkk",
            "title": "Production Overview",
            "tags": [ "templated" ],
            "timezone": "browser",
            "schemaVersion": 16,
            "version": 0
        },
        "meta": {
            "isStarred": false,
            "url": "/d/cIBgcSjkk/production-overview",
            "folderId": 2,
            "folderUid": "l3KqBxCMz",
            "slug": "production-overview" //deprecated in Grafana v5.0
        }
    }
    """
    url = gf_env.base_url + GF_EXPORT_DASH_ENDPOINT.format(uid)
    response = requests.get(url, headers=get_authorization_header(
        gf_env.auth_token), timeout=REQUEST_TIMEOUT_IN_SECONDS)

    if response.status_code != 200:
        raise ValueError(f'Failed to export dashboard {uid}: {response.text}')

    json_body: dict = response.json()
    dashboard = json_body.get('dashboard', None)
    if not dashboard:
        raise ValueError(f'Failed to export dashboard {uid}: {response.text}')

    write_to_file(path, json_body)
    print(f"Exported dashboard to {path}.")


def import_dashboard(gf_env: GrafanaEnvironment, uid: str, path: str) -> None:
    """ Import a dashboard to Grafana.

    Request body example:
    {
        "dashboard": {
            "id": null,
            "uid": null,
            "title": "Production Overview",
            "tags": [ "templated" ],
            "timezone": "browser",
            "schemaVersion": 16,
            "version": 0,
            "refresh": "25s"
        },
        "folderId": 0,
        "folderUid": "l3KqBxCMz",
        "message": "Made changes to xyz",
        "overwrite": false
    }
    """
    url = gf_env.base_url + GF_IMPORT_DASH_ENDPOINT
    export_dump = read_from_file(path)

    if 'dashboard' not in export_dump or 'meta' not in export_dump:
        raise ValueError(f'Invalid export dump: {export_dump}')

    request_body = {}
    request_body['dashboard'] = export_dump['dashboard']
    request_body['folderId'] = export_dump['meta']['folderId']
    request_body['folderUid'] = export_dump['meta']['folderUid']
    request_body['message'] = 'Imported from another Grafana instance.'
    request_body['overwrite'] = True

    response = requests.post(url, headers=get_authorization_header(gf_env.auth_token),
                             json=request_body, timeout=REQUEST_TIMEOUT_IN_SECONDS)

    if response.status_code != 200:
        raise ValueError(f'Failed to import dashboard {uid}: {response.text}')

    json_body: dict = response.json()
    print("Imported dashboard: ")
    print(json_body)


def main():
    """ Main functions. """
    gf_base_url = os.environ.get('GF_BASE_URL', 'http://localhost:3000')
    gf_auth_token = os.environ.get('GF_AUTH_TOKEN', None)

    parser = argparse.ArgumentParser(description='Migrate Grafana dashboards')
    parser.add_argument('mode', required=True, choices=['export', 'import'],
                        help='The mode of operation.')
    parser.add_argument('uid', required=True, help='The UID of the dashboard to export.')
    parser.add_argument('--path', default='.', help='The path to the export JSON file.')
    parser.add_argument('--api-key', help='The Grafana API key.')
    parser.add_argument('--base-url', help='The base URL of the Grafana instance.')
    args = parser.parse_args()

    gf_env = GrafanaEnvironment(
        base_url=args.base_url if args.base_url else gf_base_url,
        auth_token=args.api_key if args.api_key else gf_auth_token
    )

    if args.mode == 'export':
        export_dashboard(gf_env, args.uid, args.path)
    elif args.mode == 'import':
        import_dashboard(gf_env, args.uid, args.path)
    else:
        raise ValueError(f'Invalid mode of operation: {args.mode}')


if __name__ == '__main__':
    main()
