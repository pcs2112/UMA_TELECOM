"""Application configuration."""
import os
from dotenv import load_dotenv

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))

# load dotenv
dotenv_path = os.path.join(ROOT_DIR, '.env')
load_dotenv(dotenv_path)

# Set the settings

config = {
    'DB_SERVER': os.getenv('DB_HOST'),
    'DB_NAME': os.getenv('DB_NAME'),
    'DB_USER': os.getenv('DB_USER'),
    'DB_PASSWORD': os.getenv('DB_PASSWORD'),
    'DB_DRIVER': os.getenv('DB_DRIVER'),
    'DB_TRUSTED_CONNECTION': os.getenv('DB_TRUSTED_CONNECTION') == '1',
    'ROOT_DIR': ROOT_DIR,
    'TMP_DIR': os.path.join(ROOT_DIR, 'tmp'),
    'LOGS_DIR': os.path.join(ROOT_DIR, 'logs'),
    'IN_DIR': os.path.join(ROOT_DIR, 'in'),
    'OUT_DIR': os.path.join(ROOT_DIR, 'out'),
    'IS_PRODUCTION': os.getenv('PRODUCTION') == '1',
    'TELECOM_REP_ENDPOINT': 'http://lionwebapi-fr.ultimatemedical.edu/api/v2/Telecom/GetAllUsers',
    'OPSGENIE_API_KEY': os.getenv('OPSGENIE_API_KEY'),
    'OPSGENIE_GENIE_KEY': os.getenv('OPSGENIE_GENIE_KEY')
}


def get_config():
    return config
