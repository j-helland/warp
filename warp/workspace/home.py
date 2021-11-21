import os
import functools
import time

import warp.globals
import warp.constants as constants

# logging
from warp.globals import log

# types
from typing import Optional


__all__ = ['Home']


class Home:
    """
    Manager for the WARP cache directory.
    Useful for creating new sessions and for getting file/dir paths relative to the session.

    Examples:
        Get the path to the cache directory associated with a session_id.
        ```python
        session_id = 'DEV'
        home = Home(session_id=session_id)
        home()  
        ```
    """
    __slots__ = ('path', 'session_id')

    def __init__(
        self, 
        path        :str, 
        session_id  :Optional[str]  =None
    ):
        """
        Arguments:
            path: (Deprecated) Path to the WARP cache directory.
            session_id: The name of the session to create or load.
        """
        self.path = path
        if not os.path.exists(self.path):
            log.warn(f'home {os.path.abspath(self.path):s} not found, creating...')
            os.makedirs(self.path, exist_ok=True)

        self.session_id = session_id or warp.globals.SESSION_ID
        if session_id is None:
            self.session_id = str(time.time())
        else:
            session_id = str(session_id)
            self.session_id = session_id
        warp.globals.update_session_id(self.session_id)

    @property
    @functools.lru_cache(maxsize=1)
    def session_dir(self) -> str:
        """Get the unique subdirectory associated with the current run. 
        Create the session directory if not yet initialized.
        """
        id = self.session_id
        if self.session_id is None:
            subdirs = [f.path for f in os.scandir(self.path) if f.is_dir()]
            id = len(subdirs)
        elif not self.is_valid_session_id(self.session_id):
            log.warn(f'No session with id {self.session_id} found, creating...')
            id = self.session_id

        # metadata cache for session
        session_dir = os.path.join(self.path, str(id))
        os.makedirs(session_dir, exist_ok=True)

        # local cache for non-static products
        os.makedirs(os.path.join(session_dir, 'products'), exist_ok=True)

        # create a timestamp for when session was initialized
        with open(os.path.join(session_dir, constants.WARP_PIPE_TIMESTAMP_FILE_NAME), 'w') as f:
            f.write(str(time.time()))

        return session_dir

    def is_valid_session_id(self, session_id :str) -> bool:
        """Returns `True` if `session_id` has been used.

        Arguments:
            session_id: The name of the session.
        """
        return os.path.isdir(os.path.join(self.path, str(session_id)))

    def __call__(self, relpath :str) -> str:
        """
        Return the `os.path.join` of `relpath` with the WARP cache directory of the current session.
        """
        return os.path.join(self.session_dir, relpath)
