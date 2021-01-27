# Reverse Geo-Coding With Elasticsearch

## Motivation

Let's imagine that our company operates a chain of stores. In order to better understand our customer base, we've started to ingest mobility data from a third-party supplier which shows us information about the origins and destinations of our customers, namely the spatial coordinates of each user's likely home location along with the coordinates of the store they visited.

Spatial coordinates on their own might not be particualry usefeul since they're probably a little too granular for our analysts to be able to easily glean useful information from. One way to look at this data might be to aggregate up to some kind of statistical boundary, for example we might look at the proportion of customers visiting a store from various census blocks. This is more useful since we can then combine this information with government census data or other demographic data which is typically given relative to a government-defined statistcal boundary.

Therefore, as part of our ETL process we want a way to efficiently bulk map a given set of latitudes and longitudes to their corresponding census blocks. One way to do this is to use an external REST API; the problem with doing this however is that if these APIs are free, then they often come with restrictions around the number of API calls that can be made, as well as lacking any kind of performance guarantees. On the other hand, if the APIs don't restrict the volume of calls and do guarantee certain performance SLAs, then they're typically premium and therefore potentially very costly.

A simple alternative is to employ some kind of spatial database to store the various census geometries and associated data which we can then use to perform intersection comparisons between our coordinates and the various polygons. In this post we'll use Elasticsearch and Python to create a spatial index which we can then use to reverse geo-code our coordinates.

## Setting Up the Cluster

To begin, let's set up a local Elasticsearch cluster so that we can start building our index. On macOS we can install a development cluster with Homebrew:

```bash
brew tap elastic/tap
brew install elastic/tap/elasticsearch-full
```

Now let's start the cluster:

```bash
elasticsearch
```

To check whether the cluster is up we can call:

```bash
curl http://localhost:9200?pretty
```

This should return something like:

```json
{
    "name" : "Jamies-MacBook-Air.local",
    "cluster_name" : "elasticsearch_jamiehargreaves",
    "cluster_uuid" : "cv5-yuNrRiGott8ct_tvCg",
    "version" : {
        "number" : "7.10.2",
        "build_flavor" : "default",
        "build_type" : "tar",
        "build_hash" : "747e1cc71def077253878a59143c1f785afa92b9",
        "build_date" : "2021-01-13T00:42:12.435326Z",
        "build_snapshot" : false,
        "lucene_version" : "8.7.0",
        "minimum_wire_compatibility_version" : "6.8.0",
        "minimum_index_compatibility_version" : "6.0.0-beta1"
    },
  "tagline" : "You Know, for Search"
}
```

## Getting the Data

Now that Elasticsearch is ready, we need relevant census boudnaries to be able to index. Let's assume all of our stores are in England; England has the following census division hierarchy (from most to least granular):

* Output Area (OA)
* Lower Super Output Area (LSOA)
* Middle Super Output Area (MSOA)
* Local Authority District (LAD)
* Country

We can download a shapefile with output area-level geometries for the 2011 census (the most recent census when writing), from the UK Data Service [here](https://borders.ukdataservice.ac.uk/easy_download_data.html?data=England_oa_2011). This shapefile has the following structure:

| Column   | Data Type | Description                                            |
|----------|-----------|--------------------------------------------------------|
| code     | String    | Unique Output Area code                                |
| label    | String    | Output Area code plus additionally encoded information |
| name     | String    | Unpopulated                                            |
| geometry | Geometry  | patial geometry defining the Output Area               |

Since the main shapefile only contains details of the Output Area, we also need an additional lookup to link each Output Area to its parent grouping. Luckily, the UK Data Service also provides a lookup as a CSV file [here](https://borders.ukdataservice.ac.uk/lut_download_data.html?data=oa11_lsoa11_msoa11_lad11_ew_lu). The file (which also includes Welsh Output Areas), has the following structure:

| Column   | Data Type | Description                                |
|----------|-----------|--------------------------------------------|
| OA11CD   | String    | Output Area code                           |
| LSOA11CD | String    | Lower Super Output Area code               |
| LSO11ANM | String    | (Mis-spelled) Lower Super Output Area Name |
| MSOA11CD | String    | Middle Super Output Area code              |
| MSOA11NM | String    | Middle Super Output Area name              |
| LAD11CD  | String    | Local Authority District code              |
| LAD11NM  | String    | Local Authority District name              |
| LAD11NMW | String    | Welsh Local Authority District name        |

## Initial Configuration

We now have an Elasticsearch cluster along with the data we need to build our index, but before we do anything else, let's create a configuration file. There aren't many variables to configure; all we need are the Elasticsearch connection details (including the index name), as well as the location of the census shapefile and lookup table:

```python
# config.py

config = {
    'elasticsearch': {
    'ES_URL': 'http://localhost:9200',
    'ES_USER': 'elastic',
    'ES_PWD': None,
    'ES_INDEX': 'census_boundaries'
    }
    'data': {
        'CENSUS_BOUNDARIES': '../data/england_oa_2011/england_oa_2011.shp',
        'CENSUS_LOOKUP': '../data/england_oa_2011_lookup.csv'
    }
}
```

In addition to configuring the variables above, we'll also set up a logging module which will log to both the console and to a dedicated log file:

```python
# logger.py

import logging
logger = logging.getLogger('etl')
logger.setLevel(logging.INFO)

# format output
formatter = logging.Formatter('%(asctime)s - %(name)s'
                              ' - %(levelname)s - %(message)s')

# file handler
fileHandler = logging.FileHandler('../logs/etl.log', mode='w')
fileHandler.setFormatter(formatter)
fileHandler.setLevel(logging.INFO)

# console handler
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
consoleHandler.setLevel(logging.INFO)

# add handlers
logger.addHandler(fileHandler)
logger.addHandler(consoleHandler)
```

## Connecting to Elasticsearch and Reading the Data

Now that we've got our variables configured, let's make a new module and start to prepare the data ready for indexing. First, we'll import the releveant packages:

```python
from elasticsearch import Elasticsearch, helpers 
import geopandas as gpd 
import pandas as pd 
from shapely.geometry import mapping 
```

Next, we'll define some custom exception classes; one for the event that some of our geometries are invalid and another which we'll raise when creating our new Elasticsearch index if an index of the same name already exists:

```python
class InvalidGeometry(Exception): 
  pass 


class IndexAlreadyExists(Exception): 
  pass
```

Let's define a function to instantiate a connection to the Elasticsearch cluser. Since connecting is slightly different depending on whether we're connectig locally (via HTTP) or to a remote clutser over HTTPS, we'll define the connection conditionally (though we'll only be connecting over HTTP here):

```python
# etl.py

def connect_to_elastic(es_url, es_user, es_pwd):
    if 'localhost' in es_url:
        return Elasticsearch([es_url])
    else:
        return Elasticsearch(
            [es_url],
            http_auth=(es_user, es_pwd),
            scheme='https')
```

Okay, we can connect to the cluster, so now let's read in the data. For efficiency, we'll be specific about which columns we actually need. We'll drop the label and name columns of the boundary shapefile and the LAD11NMW column from the lookup file. In addition, it's always a good idea to pass in a value for the dtype argument in the pandas.read_csv method, so we'll do that here as well:

```python
# etl.py

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
        eader=0,
        ames=cols,
        secols=cols,
        type=lookup_dtype)

    return [boundaries, lookup]
```

Note that the purpose of converting our column names to generic hierarchy levels is because each country can have wildly varying statistical census boundaries and we want our index to be extensible.

## Preparing the Data and Indexing

Now we can read the data, let's define a function to prep and merge the two data sets. As well as joining the two data sets to add the higher-level groupings to each Output Area, we'll also add an additional column to define the country. In addition, we'll try to make sure our polygons are valid before we index them. To do this, we'll first add a zero buffer to each geometry in the dataframe since this is a nice trick that can often fix invalid polygons (see [here](https://stackoverflow.com/questions/20833344/fix-invalid-polygon-in-shapely) for example), then we'll check the validiy of each of the polygons and raise an exception if we still find any issues since these will need to be addressed on an individual basis before we can index.

```python
# etl.py

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
```

Now that the data is prepped, we can start to set up our index in Elasticsearch. We'll use the following mapping (or schema) in the index:

```json
"mappings": { 
    "properties": { 
        "level0_id": {"type": "keyword"}, 
        "level1_id": {"type": "keyword"}, 
        "level1_name": {"type": "keyword"}, 
        "level2_id": {"type": "keyword"}, 
        "level2_name": {"type": "keyword"}, 
        "level3_id": {"type": "keyword"}, 
        "level3_name": {"type": "keyword"}, 
        "level4_name": {"type": "keyword"}, 
        "geometry": {"type": "geo_shape"}, 
        "level_mapping": {"type": "flattened"} 
    } 
}
```

Note that we're mapping all our string fields to the keyword data type. This is different to the text data type in that keyword fields aren't analyzed (which is what we want since we're not going to perform free-text search on them). In addition, we've also added a new field called ```level_mapping```. The purpose of this field is to serve as a mapping between the generic hierarchy levels and the country-specific meaning of those levels. In the event that we decide to add a country with additional levels of granularty (e.g. the USA), we can simply add new fields to the index mapping and define the relevant translation as below. In our case, level_mapping will be a JSON object of the form:

```json
{ 
    "level0_id": "oa_code", 
    "level1_id": "lsoa_id", 
    "level1_name": "losa_name", 
    "level2_id": "msoa_id", 
    "level2_name": "msoa_name", 
    "level3_id": "lad_id", 
    "level3_name": "lad_name", 
    "level4_name": "country" 
}
```

Our full function checks whether or not the named index already exists and, if not, creates a new index with the mapping as defined above:

```python
# etl.py

def create_index(es, index):
    # check the index doesn't already exist
    exists = es.indices.exists(index)
    if exists:
        raise IndexAlreadyExists(f'Index {index} already exists')

    # settings - i.e. sharding and replication
    # note replication won't really mean much here since we're using
    # a single node dev cluster
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
```

We're nearly done with our ETL steps, but we still need to define a function to convert the rows of our dataframe into dictionaries that we can index:

```python
# etl.py

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
```

Note that this function defines a generator; this is useful because it means that for large dataframes we don't need to copy the entire thing into memory in the form of a list of dictionaries. Instead we can pass the generator function as the action to the Elasticsearch bulk method and generate the number of documents we need batch by batch. Finally, let's index our documents (we'll index 1,000 in each batch):

```python
# etl.py

def index_data(es, data, index):
    response = helpers.bulk(
        es,
        generate_docs(data, index),
        chunk_size=1_000)

    return response
```

## Executin the ETL pipeline

Now that we've defined all the steps of our ETL process, all that's left do is create another script which imports our modules and executes them whilst doing some basic exception handling and logging:

```python
# execute_etl.py

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
```

Now we can execute the ETL to create our index:

```bash
python execute_etl.py
```

After the script has finished running, we can take a look at the log file (or the console) to see something like the following:

```bash
2021-01-24 20:06:07,727 - etl - INFO - Configuring variables 
2021-01-24 20:06:07,727 - etl - INFO - Connecting to Elasticsearch 
2021-01-24 20:06:07,728 - etl - INFO - Reading data at... 
2021-01-24 20:06:22,041 - etl - INFO - Pre-processing data for indexing 
2021-01-24 20:08:51,448 - etl - INFO - Creating new index census_boundaries 
2021-01-24 20:08:52,768 - etl - INFO - Indexing data in index census_boundaries 
2021-01-24 20:17:04,634 - etl - INFO - 171372 documents of 171372 successfully indexed 
```

## Testing the Functionality

We're now in a position to actually test the functionality of our new Elasticsearch index. The example below (see ```/notebooks/example.ipynb``` in this repo) gives an example of this, reverse geo-coding the coordinates of all Manchester-based postcodes in England. Note the use of the Elasticsearch Multi-Search API; whilst this requires some very specific formatting (grouping our queries into batches of New-Line Delimited JSON), the performance gain is well worth the extra effort. The Notebook contains a comparison between the Multi-Search API and naive looping with the Search API for 1,000 queries, with the Multi-Search API performing roughly nine times faster:

```python
from elasticsearch import Elasticsearch, helpers
import json
import pandas as pd

# connect to Elasticsearch
es = Elasticsearch(['http://localhost:9200'])

# load sample data
sample = pd.read_csv('../data/ukpostcodes.csv',
                     usecols=['postcode', 'latitude', 'longitude'])

# filter to Manchester postcodes
man_postcodes = sample[sample.postcode.str.contains('^M\d+')].reset_index(drop=True)

# construct Multi-Search API call
# list of queries
queries = []
for row in man_postcodes.itertuples():
    lon = row.longitude
    lat = row.latitude
    query = {
        'size': 1,
        '_source': {'exclude': ['geometry', 'level1*', 'level2*', 'level3_id', 'level_mapping']},
        'query': {
            'bool': {
                'filter': {
                    'geo_shape': {
                        'geometry': {
                            'shape': {
                                'type': 'point',
                                'coordinates': [lon, lat]
                            }
                        }
                    }
                }
            }
        }
    }
    queries.append(query)

# convert each dict to a string
queries_str = [json.dumps(query) for query in queries]

# split list into batches of 1,000 queries
def make_batches(lst, batch_size):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]
batches = [batch for batch in make_batches(queries_str, 1_000)]

# format as new line delimited JSON
ndjson_batches = ['{}\n' + '\n{}\n'.join(batch) + '\n' for batch in batches]


# ----- returns ~ 520ms ----- #
%%timeit
# TEST on 1,000 docs - query Elasticsearch with Multi-Search API
results = es.msearch(body=ndjson_batches[0], index='census_boundaries')

# ----- returns ~ 4.6s ----- #
%%timeit
# TEST - query Elasticsearch with Search API in a for-loop
results = [es.search(body=queries[i], index='census_boundaries') for i, _ in enumerate(queries[0:999])]


# query Elasticsearch with Multi-Search API
results = [es.msearch(body=batch, index='census_boundaries') for batch in ndjson_batches]

# parse results
responses = [result.get('responses') for result in results]
source = [item.get('hits').get('hits')[0].get('_source') for response in responses 
                                                         for item in response]

# convert to dataframe
census_blocks = pd.DataFrame(source)

# column bind with original dataframe
mapped_postcodes = pd.concat([man_postcodes, census_blocks], axis='columns')
mapped_postcodes = mapped_postcodes.loc[:, ['postcode', 'level4_name', 'level3_name', 
                                            'level0_id', 'latitude', 'longitude']]
```

## Closing Thougths

We've seen that by using an Elasticsearch back-end, we can easily create a performant and extensible way to bulk reverse geo-code spatial coordinates down to a granularity of our choosing (in this case census Output Areas), which can then be used in a wider ETL process to provide analysts and data scientists with a way to effectively analyse spatial data at standardised levels of aggregation.
