from config import config
import etl
import logging

def main():
    # configure logger
    logging.basicConfig(
        filename = '../logs/etl.log',
        level = logging.INFO,
        format = '%(asctime)s - %(levelname)s - %(message)s',
        force = True
    )

    # set up config variables
    logging.info('Configuring variables')
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
        logging.error(f'Missing config variable {e}')
        raise KeyError(f'Missing config variable {e}')

    # read data
    logging.info(f'Reading data at "{CENSUS_BOUNDARIES}" and "{CENSUS_LOOKUP}"')
    boundaries, lookup = etl.read_data(CENSUS_BOUNDARIES, CENSUS_LOOKUP)

    # prep the data for indexing
    logging.info('Pre-processing data for indexing')
    prepped_data = etl.prep_data(boundaries, lookup)

    # create the new index
    logging.info(f'Creating new index "{ES_INDEX}"')
    exists = etl.index_exists(ES_URL, ES_USER, ES_PWD, INDEX_NAME)
    if exists:
        logging.error(f'Index {ES_INDEX} already exists')
        raise Exception(f'Index {ES_INDEX} already exists')

    etl.create_index(ES_URL, ES_USER, ES_PWD, INDEX_NAME)

    # add data to the index
    response = etl.index_data()

if __name__ == '__main__':
    main()