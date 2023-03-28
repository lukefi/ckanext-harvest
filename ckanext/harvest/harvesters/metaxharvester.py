from ckan.lib.helpers import json
from ckan.logic import ValidationError
from ckan import model

from ckanext.harvest.model import HarvestObject
from .base import HarvesterBase

import requests
import itertools
import re

from datetime import datetime, date, timedelta, timezone
from urllib.parse import urlencode, urlparse, urljoin

import logging
log = logging.getLogger(__name__)

DATASETS_PATH = '/rest/v2/datasets'
ACCESS_TYPE_OPEN = 'http://uri.suomi.fi/codelist/fairdata/access_type/code/open'

class MetaxHarvester(HarvesterBase):
    '''
    A Harvester for Metax instances
    '''

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
        harvest_object = HarvestObject.get(harvest_object_id)
        guid = harvest_object.guid

        base_url = harvest_object.job.source.url
        url = '/'.join(part.strip('/') for part in (base_url, DATASETS_PATH, guid))
        return url

    def gather_stage(self, harvest_job):
        log.debug(f'In MetaxHarvester gather_stage ({harvest_job.source.url})')

        last_error_free_job = self.last_error_free_job(harvest_job)
        log.info(f'Last error-free job: {last_error_free_job}')
        metax_datasets_url = '/'.join(part.strip('/') for part in (harvest_job.source.url, DATASETS_PATH))

        try:
            datasets = search_for_datasets(metax_datasets_url)
            log.info(f'Received metadata for {len(datasets)} datasets')
            if not datasets:
                self._save_gather_error(
                    f'No datasets found at Metax: {metax_datasets_url}',
                    harvest_job
                )
                return []
        except SearchError as e:
            log.info(f'Searching for all datasets gave an error: {e}')
            self._save_gather_error(
                f'Unable to search Metax for datasets: {e} url: {metax_datasets_url}',
                harvest_job
            )
            return None

        datasets_to_update = (ds for ds in datasets if needs_updating(ds, last_error_free_job))
        try:
            harvest_objects = [
                HarvestObject(
                    guid=dataset['identifier'],
                    job=harvest_job,
                    content=json.dumps(dataset),
                    metadata_modified_date=dataset.get('date_modified')
                )
                for dataset in datasets_to_update
            ]

            for obj in harvest_objects:
                obj.save()

            return [obj.id for obj in harvest_objects]
        except Exception as e:
            self._save_gather_error(f'Creating harvest objects failed: {e}', harvest_job)

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
            dataset_dict = json.loads(harvest_object.content)
            package_dict = self._convert_to_package_dict(dataset_dict)
            result = self._create_or_update_package(package_dict, harvest_object, package_dict_form='package_show')
            return result
        except ValidationError as e:
            self._save_object_error(
                f'Invalid package with GUID {harvest_object.guid}: {e.error_dict}',
                harvest_object, 'Import'
            )
            return False
        except Exception as e:
            self._save_object_error(f'{e}', harvest_object, 'Import')
            return False

    def _get_license_id(self, dataset):
        licenses = dataset.get('research_dataset', {}).get('access_rights', {}).get('license')
        if not licenses:
            return 'notspecified'
        url = licenses[0].get('identifier')
        license_id = url_to_license_id(url)
        if license_id in self.license_register.keys():
            return license_id
        else:
            return None

    def _convert_to_package_dict(self, dataset_dict):
        identifier = dataset_dict.get('identifier')
        research_dataset = dataset_dict['research_dataset']
        license_id = self._get_license_id(dataset_dict)

        return {
            'id': identifier,
            'name': research_dataset.get('preferred_identifier'),
            'title': get_preferred_language_version(research_dataset.get('title')),
            'url': f'https://etsin.fairdata.fi/dataset/{identifier}',
            'author': get_author_string(research_dataset.get('creator', [])),
            'maintainer': get_contributor_name(research_dataset.get('publisher', {})),
            'notes': get_preferred_language_version(research_dataset.get('description')),
            'metadata_created': datetime.fromisoformat(dataset_dict.get('date_created')),
            'metadata_updated': datetime.fromisoformat(dataset_dict.get('date_modified', dataset_dict.get('date_created'))),
            # metadata_modified determines if the package needs to be updated
            'metadata_modified': dataset_dict.get('date_modified'),
            'tags': [{'name':  kw} for kw in research_dataset.get('keyword', [])],
            'groups': get_groups(research_dataset.get('keyword', [])),
            'resources': [
                convert_resource(resource) for resource in research_dataset.get('remote_resources', [])
            ] or [
                generic_resource(identifier)
            ],
            'extras': [
                {
                    'key': 'Julkaisupäivämäärä',
                    'value': date.fromisoformat(research_dataset.get('issued'))
                }
            ],
            'license_id': license_id,
            'owner_org': 'luke'
        }


def needs_updating(dataset, last_error_free_job):
    if not last_error_free_job:
        return True
    updated_in_metax = datetime.fromisoformat(dataset.get('date_modified', dataset.get('date_created')))
    # gather_started is created with datetime.utcnow() but the datetime object
    # is not aware of the time zone
    updated_in_ckan = last_error_free_job.gather_started.replace(tzinfo=timezone.utc)
    buffer = timedelta(hours=1)
    return (updated_in_metax - updated_in_ckan) + buffer > timedelta()


def generic_resource(identifier):
    return {
        'name': 'Lataa aineisto / ladda ner data / download the data',
        'url': f'https://etsin.fairdata.fi/dataset/{identifier}/data'
    }


def convert_resource(resource):
    name = resource.get('title')
    url, description = pick_urls(resource)
    format = infer_resource_format(url)
    return {
        'name': name,
        'url': url,
        'description': description,
        'format': format
    }


def pick_urls(resource):
    download_url = resource.get('download_url', {}).get('identifier')
    access_url = resource.get('access_url', {}).get('identifier')
    if download_url and access_url:
        return download_url, f'Lisätietoja / mer information / more information: {access_url}'
    else:
        return download_url or access_url, None


def search_for_datasets(remote_base_url, query_params=None):
    '''
    Does a dataset search on Metax. Deals with paging.
    '''

    params = {
        **(query_params if query_params else {}),
        'latest': 'true',
        'metadata_owner_org': 'luke.fi',
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

    open_datasets = [ds for ds in itertools.chain(*pages) if ds.get('research_dataset', {}).get('access_rights', {}).get('access_type', {}).get('identifier') == ACCESS_TYPE_OPEN]

    return filter_duplicates(open_datasets)


def filter_duplicates(datasets):
    ids = set()
    filtered_datasets = []

    for ds in datasets:
        if ds['id'] in ids:
            log.info(f'Discarding duplicate dataset {ds["id"]} - probably due '
                     'to datasets being changed at the same time as '
                     'when the harvester was paging through')
        else:
            ids.add(ds['id'])
            filtered_datasets.append(ds)

    return filtered_datasets


def _getContent(url):
    response = requests.get(url)
    response.raise_for_status()
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


def get_groups(keywords):
    groups = []
    if 'meritietoportaali' in keywords:
        groups.append({'name': 'meritieto'})
    return groups


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
