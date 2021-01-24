import re
import geopandas as gpd
import pandas as pd
from elasticsearch import Elasticsearch, helpers

def connect_to_elastic(es_url, es_user, es_pwd):
    if 'localhost' in es_user:
        es = Elasticsearch([es_url])
    else:
        es = Elasticsearch(
            [es_url],
            http_auth = (es_user, es_pwd),
            scheme = 'https')

def read_data(boundary_file, lookup_file):
    boundaries = gpd.read_file(boundary_file)
    lookup = pd.read_csv(lookup_file)
    return [boundaries, lookup]

def prep_data(boundaries, lookup):
    # drop useless columns
    boundaries.drop(['label', 'name'], axis = 'columns', inplace = True)
    lookup.drop(['LAD11NMW'], axis = 'columns', inplace = True)

    # rename to generic hierarchy levels
    boundaries.rename({'code': 'level0_id'}, inplace = True)
    lookup.rename(
        {
            'OA11CD': 'level0_id',
            'LSOA11CD': 'level1_id',
            'LSO11ANM': 'level1_name',
            'MSOA11CD': 'level2_id',
            'MSOA11NM': 'level2_name',
            'LAD11CD': 'level3_id',
            'LAD11NM': 'level3_name'
        },
        inplace = True)

    # join data
    prepped_data = pd.merge(boundaries, lookup, how = 'left', on = 'level0_id')
    return prepped_data

def index_exists(es, index):
    exists = es.indices.exists(index = index)
    return exists

def create_index(es, index):
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
        index = index,
        body = {
            'settings': settings,
            'mappings': mappings
        })   

def generate_docs(data, index):
    for row in data.itertuples():
        yield {
            '_index': index,
            '_id': f'{level4_name}-{row.level0_id}'
            '_type': '_doc',
            '_source': {
                'level0_id': row.level0_id,
                'level1_id': row.level1_id,
                'level1_name': row.level1_name,
                'level2_id': row.level2_id,
                'level2_name': row.level2_name,
                'level3_id': row.level3_id,
                'level3_name': row.level3_name,
                'level4_name': 'England',
                'geometry': row.geometry,
                'level_mapping': {
                    'level0_id': 'oa_code',
                    'level1_id': 'lsoa_code',
                    'level1_name': 'lsoa_name',
                    'level2_id': 'msoa_id',
                    'level2_name': 'msoa_name',
                    'level3_id': 'lad_id',
                    'level3_id': 'lad_name',
                    'level4_name': 'country'
                }
            }
        }

def index_data(es, data, index):
    response = helpers.bulk(
        es,
        generate_docs(data, index),
        max_chunk_bytes = 10_000)
    
    return response
