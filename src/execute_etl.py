from config import config
import etl
from logger import logger
import os


def main():
    # set up config variables
    logger.info('Configuring variables')
    try:
        # assign config parents
        es_config = config['elasticsearch']
        data_config = config['data']

        # elasticsearch
        ES_URL = es_config['ES_URL']
        ES_USER = es_config['ES_USER']
        ES_PWD = es_config['ES_PWD']
        ES_INDEX = es_config['ES_INDEX']

        # data
        CENSUS_BOUNDARIES = data_config['CENSUS_BOUNDARIES']
        CENSUS_LOOKUP = data_config['CENSUS_LOOKUP']

    except KeyError as e:
        logger.error(f'Missing config variable {e}')
        raise KeyError(f'Missing config variable {e}')

    # create elasticsearch connection
    logger.info('Connecting to Elasticsearch')
    es = etl.connect_to_elastic(ES_URL, ES_USER, ES_PWD)

    # read data
    logger.info(f'Reading data at {os.path.abspath(CENSUS_BOUNDARIES)} '
                f'and {os.path.abspath(CENSUS_LOOKUP)}')
    boundaries, lookup = etl.read_data(CENSUS_BOUNDARIES, CENSUS_LOOKUP)

    # prep data ready for indexing
    logger.info('Pre-processing data for indexing')
    try:
        prepped_data = etl.prep_data(boundaries, lookup)
    except etl.InvalidGeometry as e:
        logger.error(e)
        raise

    # check the new index doesn't already exist, create if not
    logger.info(f'Creating new index {ES_INDEX}')
    try:
        etl.create_index(es, ES_INDEX)
    except etl.IndexAlreadyExists as e:
        logger.error(e)
        raise

    # add data to the index
    logger.info(f'Indexing data in index {ES_INDEX}')
    try:
        response = etl.index_data(es, prepped_data, ES_INDEX)
        logger.info(f'{response[0]} of {prepped_data.shape[0]} documents '
                    f'successfully indexed')
    except Exception as e:
        logger.error(f'Indexing failed with error {e}')
        raise


if __name__ == '__main__':
    main()
