""" Handles all API calls. """
from typing import Optional
from dataclasses import dataclass
from urllib import parse

import requests

REQUEST_TIMEOUT_IN_SECONDS = 5
GF_EXPORT_DASH_ENDPOINT = "/api/dashboards/uid/{}"
GF_IMPORT_DASH_ENDPOINT = "/api/dashboards/db"
GF_SEARCH_DASH_ENDPOINT = "/api/search"
GF_FOLDERS_ENDPOINT = "/api/folders"
GF_GET_DATASOURCE_ENDPOINT = "/api/datasources"


@dataclass
class GrafanaEnvironment:
    """ Grafana environment variables. """
    base_url: str
    auth_token: str


class GrafanaAPIHandler:
    """ Handles all API calls. """

    def __init__(self, gf_env: GrafanaEnvironment, requests_module=requests):
        self.requests = requests_module
        self.gf_env = gf_env

    def export_dashboard(self, uid: str) -> dict:
        """ Export a dashboard from Grafana. """
        url = self.gf_env.base_url + GF_EXPORT_DASH_ENDPOINT.format(uid)
        response = self.requests.get(
            url,
            headers=self.__auth_header,
            timeout=REQUEST_TIMEOUT_IN_SECONDS)

        if response.status_code != 200:
            raise ValueError(
                f'Failed to export dashboard {uid}: {response.text}')

        json_body: dict = response.json()
        if json_body.get('dashboard', None) is None:
            raise ValueError(
                f'Failed to export dashboard {uid}: {response.text}')
        return json_body['dashboard']

    def import_dashboard(self, export_dump: dict, folder_id: int) -> None:
        """ Import a dashboard to Grafana. """

        request_body = {}
        request_body['dashboard'] = export_dump
        request_body['folderId'] = folder_id
        request_body['message'] = 'Imported from another Grafana instance.'
        request_body['overwrite'] = True

        url = self.gf_env.base_url + GF_IMPORT_DASH_ENDPOINT
        response = self.requests.post(url,
                                      headers=self.__auth_header,
                                      json=request_body,
                                      timeout=REQUEST_TIMEOUT_IN_SECONDS)

        if response.status_code != 200:
            raise ValueError(
                f'Failed to import dashboard: {response.text}')

        json_body: dict = response.json()
        print(f"Imported dashboard: {json_body['url']}")

    def list_dashboards(self) -> dict:
        """List only dashboards using grafana API. """
        response = self.requests.get(self.gf_env.base_url + GF_SEARCH_DASH_ENDPOINT +
                                     '?type=dash-db', # dashboard filter excludes folders
                                     headers=self.__auth_header,
                                     timeout=REQUEST_TIMEOUT_IN_SECONDS)

        if response.status_code != 200:
            raise ValueError("Failed to list dashboards")
        return response.json()

    def get_folder_id(self, folder_name: str) -> Optional[int]:
        """ Gets the ID of a specific folder. """
        if folder_name == 'General':
            return 0
        response = self.requests.get(self.gf_env.base_url + GF_FOLDERS_ENDPOINT,
                                        headers=self.__auth_header,
                                        timeout=REQUEST_TIMEOUT_IN_SECONDS)

        if response.status_code != 200:
            raise ValueError("Failed to list folders")

        folders: list[dict] = response.json()
        for folder in folders:
            if folder['title'] == folder_name:
                return folder['id']
        return None

    def create_folder(self, folder_name: str) -> int:
        """ Creates a new folder and returns its id. """
        request_body = {}
        request_body['title'] = folder_name

        response = self.requests.post(self.gf_env.base_url + GF_FOLDERS_ENDPOINT,
                                        headers=self.__auth_header,
                                        json=request_body,
                                        timeout=REQUEST_TIMEOUT_IN_SECONDS)

        if response.status_code != 200:
            raise ValueError(f"Failed to create folder {folder_name}: {response.text}")
        new_folder: dict = response.json()
        return new_folder['id']

    def get_datasource_uid(self) -> str:
        """ Get the datasource uid. """
        response = self.requests.get(self.gf_env.base_url + GF_GET_DATASOURCE_ENDPOINT,
                                     headers=self.__auth_header,
                                     timeout=REQUEST_TIMEOUT_IN_SECONDS)
        if response.status_code != 200:
            raise ValueError(
                f'Failed to get datasource uid: {response.text}')
        json_body: list[dict] = response.json()
        mongodb_source = [source for source in json_body if source['type'] == 'boschcom-devcloudmongodbgrafanaplugin-datasource'][0]
        return mongodb_source['uid']

    @property
    def __auth_header(self) -> dict:
        """ Get the authorization header. """
        return {"Authorization": f"Bearer {self.gf_env.auth_token}"}
