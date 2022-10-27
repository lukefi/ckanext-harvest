import datetime
import requests
import itertools
import re

from datetime import datetime, date
from urllib.parse import urlencode, urlparse, urljoin

import logging
log = logging.getLogger(__name__)

from ckan.lib.helpers import json
from ckan import model

from ckanext.harvest.model import HarvestObject
from .base import HarvesterBase


print('####### Loading MetaxHarvester #######')

class MetaxHarvester(HarvesterBase):
    '''
    A Harvester for Metax instances
    '''
    config = None

    api_version = 2

    license_register = model.license.LicenseRegister()

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
        log.debug('In MetaxHarvester gather_stage (%s)',
                  harvest_job.source.url)

        get_all_packages = True

#       last_error_free_job = self.last_error_free_job(harvest_job)
#       log.info('Last error-free job: %r', last_error_free_job)
#       if (last_error_free_job and
#               not self.config.get('force_all', False)):
#           get_all_packages = False

#           last_time = last_error_free_job.gather_started

#           get_changes_since = \
#               (last_time - datetime.timedelta(hours=1)).isoformat()
#           log.info('Searching for datasets modified since: %s UTC',
#                    get_changes_since)

        if get_all_packages:
            datasets = search_for_datasets(harvest_job.source.url)
            log.info(f'Received metadata for {len(datasets)} datasets')
#       else:
            # Get only those datasets that have been updated after last error-free job

        harvest_objects = [ HarvestObject(guid=dataset['identifier'], job=harvest_job, content=json.dumps(dataset)) for dataset in datasets]

        for obj in harvest_objects:
            obj.save()

        return [ obj.id for obj in harvest_objects ]

    def fetch_stage(self, harvest_object):
        # Nothing to do here. We got everything in the gather stage. If we want to
        # to fetch only updated entries, we could possibly move fetching the
        # whole data here and only get id and update time and do the filtering
        # in the gather stage.
        return True

    def import_stage(self, harvest_object):
        log.debug('In MetaxHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False

        if harvest_object.content is None:
            self._save_object_error('Empty content for object %s' %
                                    harvest_object.id,
                                    harvest_object, 'Import')
            return False

        try:
            dataset_dict = (json.loads(harvest_object.content))
            package_dict = self._convert_to_package_dict(dataset_dict)
            result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')
            return result
        except Exception as e:
            self._save_object_error(f'{e}', harvest_object, 'Import')
            return False

    def _get_license_id(self, dataset):
        licenses = dataset['research_dataset']['access_rights'].get('license')
        if not licenses:
            return 'notspecified'
        url = licenses[0].get('identifier')
        license_id = url_to_license_id(url)
        if license_id in self.license_register.keys():
            return license_id
        else:
            return None

    def _convert_to_package_dict(self, dataset_dict):
        research_dataset = dataset_dict['research_dataset']
        license_id = self._get_license_id(dataset_dict)
        return {
            'id': dataset_dict.get('identifier'),
            'name': research_dataset.get('preferred_identifier'),
            'title': get_preferred_language_version(research_dataset['title']),
            'url': dataset_dict.get('identifier'),
            # TODO: handle several or zero authors
            'author': get_author_string(research_dataset.get('creator', [])),
            'maintainer': get_contributor_name(research_dataset.get('publisher', {})),
            'notes': get_preferred_language_version(research_dataset['description']),
            'metadata_created': datetime.fromisoformat(dataset_dict.get('date_created')),
            'metadata_updated': datetime.fromisoformat(dataset_dict.get('date_modified')),
            'tags': [{'name':  kw} for kw in research_dataset.get('keyword', [])],
            'resources': [{
                'name': resource.get('title'),
                'url': resource.get('download_url', {}).get('identifier', ''),
                'format': infer_resource_format(resource.get('download_url', {}).get('identifier', ''))
            } for resource in research_dataset.get('remote_resources', [])],
            'extras': [
                {'key': 'Julkaisupäivämäärä', 'value': date.fromisoformat(research_dataset.get('issued'))}
            ],
            'license_id': license_id,
            'owner_org': 'luke-fi'
        }


def search_for_datasets(remote_base_url, query_params=None):
    '''
    Does a dataset search on Metax. Deals with paging.
    '''
    # TODO: Be prepared for datasets being added or removed during the
    # paging through them. See ckanharvester.py.

    params = {
        **(query_params if query_params else {}),
        'latest': 'true',
        'metadata_owner_org': 'luke.fi',
        #'fields': ','.join(['id', 'identifier', 'date_modified']),
        #'ordering': 'id',  # gives "Internal Server Error" if used together with 'fields'
        'limit': 10
    }

    next_url = remote_base_url + '?' + urlencode(params)
    pages = []
    while (next_url):
        try:
            page = _getContent(next_url)
            next_url = page.get('next')
            pages.append(page.get('results'))
        except Exception as e:
            raise SearchError(f'Error listing metax data: {e}')

    return list(itertools.chain(*pages))


def _getContent(url):
    response = requests.get(url)
    return response.json()


def get_author_string(creator_dicts):
    if not creator_dicts:
        return None
    author_names = (get_contributor_name(c) for c in creator_dicts)
    return ', '.join(author_names)


def get_contributor_name(creator_dict):
    current_level = creator_dict
    names = []
    while current_level:
        names.append(get_preferred_language_version(current_level.get('name')))
        current_level = current_level.get('is_part_of')

    return ' / '.join(reversed(names))


def url_to_license_id(url):
    version_number_pattern = r'\d+(\.\d+)*'
    license_part = url.split('/')[-1]
    subparts = license_part.split('-')
    if re.fullmatch(version_number_pattern, subparts[-1]):
        subparts.pop(-1)
    return '-'.join(subparts).lower()


def get_preferred_language_version(translatedEntry):
    if type(translatedEntry) == str:
        return translatedEntry
    if not translatedEntry:
        return '[missing]'
    return translatedEntry.get('fi') or translatedEntry.get('en', '---')


def infer_resource_format(url):
    if 'format=json' in url.lower():
        return 'JSON'
    if 'service=wms' in url.lower():
        return 'WMS'
    if 'outputformat=shape-zip' in url.lower():
        return 'ZIP'
    if 'outputformat=shape-zip' in url.lower():
        return 'ZIP'

    processed_url = urljoin(url, urlparse(url).path).lower()
    if processed_url.endswith('.pdf'):
        return 'PDF'
    if processed_url.endswith('.csv'):
        return 'CSV'
    if processed_url.endswith('.xls'):
        return 'XLS'
    if processed_url.endswith('.xlsx'):
        return 'XLSX'
    if processed_url.endswith('.txt'):
        return 'TXT'
    if processed_url.endswith('.doc'):
        return 'DOCX'
    if processed_url.endswith('.zip'):
        return 'ZIP'
    if processed_url.endswith('.tiff') or processed_url.endswith('.tif'):
        return 'TIFF'
    # CKAN will do its own inference if none of the above matches


class SearchError(Exception):
    pass

class DebuggingError(Exception):
    pass