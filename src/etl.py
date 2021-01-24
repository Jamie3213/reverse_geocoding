from elasticsearch import Elasticsearch, helpers
import geopandas as gpd
import pandas as pd
from shapely.geometry import mapping


# custom exceptions
class InvalidGeometry(Exception):
    pass


class IndexAlreadyExists(Exception):
    pass


def connect_to_elastic(es_url, es_user, es_pwd):
    if 'localhost' in es_url:
        return Elasticsearch([es_url])
    else:
        return Elasticsearch(
            [es_url],
            http_auth=(es_user, es_pwd),
            scheme='https')


def read_data(boundary_file, lookup_file):
    boundaries = gpd.read_file(
        boundary_file,
        ignore_fields=['label', 'name'])

    lookup_dtype = {
        'level0_id': str,
        'level1_id': str,
        'level1_name': str,
        'level2_id': str,
        'level2_name': str,
        'level3_id': str,
        'level3_name': str
    }
    cols = ['level0_id', 'level1_id', 'level1_name', 'level2_id',
            'level2_name', 'level3_id', 'level3_name']
    lookup = pd.read_csv(
        lookup_file,
        header=0,
        names=cols,
        usecols=cols,
        dtype=lookup_dtype)

    return [boundaries, lookup]


def prep_data(boundaries, lookup):
    # rename to generic hierarchy levels
    boundaries.rename(columns={'code': 'level0_id'}, inplace=True)

    # join data
    merged_data = pd.merge(boundaries, lookup, how='left', on='level0_id')
    merged_data['level4_name'] = 'England'

    # check and attempt to fix invalid geometries
    merged_data['geometry'] = merged_data.geometry.buffer(0)
    invalid_geoms = [row.level0_id for row in merged_data.itertuples()
                     if not row.geometry.is_valid]
    geoms_as_str = ', '.join(invalid_geoms)
    if invalid_geoms:
        raise InvalidGeometry(f'Found {len(invalid_geoms)} '
                              f'invalid geometries: {geoms_as_str}')

    # convert to WGS84 projection
    merged_data.to_crs('EPSG:4326', inplace=True)

    return merged_data


def create_index(es, index):
    # check the index doesn't already exist
    exists = es.indices.exists(index)
    if exists:
        raise IndexAlreadyExists(f'Index {index} already exists')

    # settings - i.e. sharding and replication
    settings = {
        'number_of_shards': 1,
        'number_of_replicas': 1
    }

    # define schema mapping
    mappings = {
        'properties': {
            'level0_id': {'type': 'keyword'},
            'level1_id': {'type': 'keyword'},
            'level1_name': {'type': 'keyword'},
            'level2_id': {'type': 'keyword'},
            'level2_name': {'type': 'keyword'},
            'level3_id': {'type': 'keyword'},
            'level3_name': {'type': 'keyword'},
            'level4_name': {'type': 'keyword'},
            'geometry': {'type': 'geo_shape'},
            'level_mapping': {'type': 'flattened'}
        }
    }

    es.indices.create(
        index=index,
        body={
            'settings': settings,
            'mappings': mappings
        })


def generate_docs(data, index):
    for row in data.itertuples():
        yield {
            '_index': index,
            '_id': f'{row.level4_name}-{row.level0_id}',
            '_source': {
                'level0_id': row.level0_id,
                'level1_id': row.level1_id,
                'level1_name': row.level1_name,
                'level2_id': row.level2_id,
                'level2_name': row.level2_name,
                'level3_id': row.level3_id,
                'level3_name': row.level3_name,
                'level4_name': row.level4_name,
                'geometry': mapping(row.geometry),
                'level_mapping': {
                    'level0_id': 'oa_code',
                    'level1_id': 'lsoa_code',
                    'level1_name': 'lsoa_name',
                    'level2_id': 'msoa_id',
                    'level2_name': 'msoa_name',
                    'level3_id': 'lad_id',
                    'level3_name': 'lad_name',
                    'level4_name': 'country'
                }
            }
        }


def index_data(es, data, index):
    response = helpers.bulk(
        es,
        generate_docs(data, index),
        chunk_size=1_000)

    return response
