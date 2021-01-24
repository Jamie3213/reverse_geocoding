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
