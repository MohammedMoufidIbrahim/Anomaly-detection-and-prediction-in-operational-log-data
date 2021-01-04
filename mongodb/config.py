import logging

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)
# LOGGER.setLevel(logging.INFO)


DB_URL = 'mongodb://admin:secret@localhost:27888/?authSource=admin'
DB_NAME = 'logs'
