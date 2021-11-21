# std
import os
import time 

# warp
import warp.constants as constants

# logging
import logging
from rich.logging import RichHandler

# types
from typing import Dict
from types import FunctionType


FORMAT = '%(message)s'
logging.basicConfig(
    level='NOTSET', format=FORMAT, datefmt='[%Y-%m-%d %H:%M:%S]', handlers=[RichHandler()])
log = logging.getLogger('rich')

def log_and_raise(e):
    log.exception(e)
    raise e


GRAPH_BUILDERS: Dict[str, FunctionType] = dict()
def register_graph(name: str =None) -> FunctionType:
    if name in GRAPH_BUILDERS:
        log_and_raise(NameError(f'`{name}` has already been registered.'))

    def decorator(func: FunctionType) -> FunctionType:
        GRAPH_BUILDERS[name or func.__name__] = func
        return func

    return decorator


SESSION_ID: str = str(time.time())
def update_session_id(session_id: str) -> None:
    global SESSION_ID
    SESSION_ID = session_id


def product_dir() -> str:
    return os.path.join(constants.HOME_DIR_DEFAULT, SESSION_ID, constants.PRODUCTS_DIR_NAME)
