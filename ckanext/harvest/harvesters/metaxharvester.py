import datetime
from urllib.parse import urlencode
import requests

import logging
log = logging.getLogger(__name__)

from .base import HarvesterBase

class MetaxHarvester(HarvesterBase):
    '''
    A Harvester for Metax instances
    '''
    config = None

    api_version = 2

    def info(self):
        return {
            'name': 'metax',
            'title': 'Metax',
            'description': 'Harvests remote Metax instances'
        }

    def validate_config(self, config):
        # optional
        pass

    def get_original_url(self, harvest_object_id):
        # optional
        pass

    def gather_stage(self, harvest_job):
        # TODO: required
        log.info('In MetaxHarvester gather_stage (%s)',
                  harvest_job.source.url)

        remote_metax_base_url = harvest_job.source.url.rstrip('/')

        last_error_free_job = self.last_error_free_job(harvest_job)
        log.info('Last error-free job: %r', last_error_free_job)

        if (last_error_free_job and
                not self.config.get('force_all', False)):
            get_all_packages = False

            last_time = last_error_free_job.gather_started

            get_changes_since = \
                (last_time - datetime.timedelta(hours=1)).isoformat()
            log.info('Searching for datasets modified since: %s UTC',
                     get_changes_since)

        return []

    def fetch_stage(self, harvest_object):
        # TODO: required
        return 'unchanged'

    def import_stage(self, harvest_object):
        # TODO: required
        return 'unchanged'

    def _search_for_datasets(self, remote_base_url, query_params=None):
        '''
        Does a dataset search on Metax. Deals with paging.
        '''
        params = {
            **(query_params if query_params else {}),
            'latest': 'true',
            'metadata_owner_org': 'luke.fi',
            'fields': ','.join(['id', 'date_modified']),
            #'research_dataset_fields': ','.join(['preferred_identifier']),
            #'ordering': 'id',  # gives "Internal Server Error" if used together with 'fields'
            'limit': 10
        }

        next_url = remote_base_url + '?' + urlencode(params)
        while (next_url):
            print(f'next url: {next_url}')
            try:
                content = self._getContent(next_url)
                print(content)
                next_url = content.get('next', None)
            except Exception as e:
                raise SearchError(f'Error listing metax data: {e}')



    def _getContent(url):
        response = requests.get(url)
        return response.content.json()


class SearchError(Exception):
    pass
