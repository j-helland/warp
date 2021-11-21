"""
"""
import os


HOME_DIR_DEFAULT = '.warp'
if 'HOME_DIR_DEFAULT' in os.environ:
    HOME_DIR_DEFAULT = os.environ['HOME_DIR_DEFAULT']

if 'WARP_HOME_DIR' in os.environ:
    path = os.path.abspath(os.environ['WARP_HOME_DIR'])
    assert os.path.isdir(path)
    HOME_DIR_DEFAULT = os.path.join(path, HOME_DIR_DEFAULT)

WARP_PORT = '8050'
if 'WARP_PORT' in os.environ:
    WARP_PORT = os.environ['WARP_PORT']

WARP_HOST_NAME = 'localhost'
if 'HOSTNAME' in os.environ:
    WARP_HOST_NAME = os.environ['HOSTNAME']

STATIC_PRODUCTS_DIR_NAME = 'static_products'
PRODUCTS_DIR_NAME = 'products'
METADATA_FILE_NAME = 'metadata.csv'
PARAMETER_FILE_NAME = 'parameters.yml'
SOURCE_FILE_NAME = 'source.txt'
PIPE_DECLARATION_NAME = 'Main'
WARP_METADATA_FILE_NAME = 'meta.warp'  # used to store the name of the most recent session
WARP_PIPE_TIMESTAMP_FILE_NAME = 'timestamp.warp'  # used to store a timestamp of when a session was last opened

WARP_LOGO = """
 _ _ _ _____ _____ _____ 
| | | |  _  | __  |  _  |
| | | |     |    -|   __|
|_____|__|__|__|__|__|   
"""
