from datetime import datetime
from typing import Optional
from tinydb import Storage

class Middleware:
    """
    The base class for all Middlewares.

    Middlewares hook into the read/write process of TinyDB allowing you to
    extend the behaviour by adding caching, logging, ...
    """

    def __init__(self, storage_cls) -> None:
        self.storage = storage_cls()

    def read(self):
        return self.storage.read()

    def write(self, data):
        self.storage.write(data)

    def close(self):
        self.storage.close()


class CachingMiddleware(Middleware):
    """
    Add some caching to TinyDB.

    This Middleware aims to improve the performance of TinyDB by writing only
    the last DB state every :attr:`WRITE_CACHE_SIZE` time and reading always
    from cache.
    """

    WRITE_CACHE_SIZE = 1000

    def __init__(self, storage_cls):
        super().__init__(storage_cls)
        self.cache = None
        self._cache_modified_count = 0

    def read(self):
        if self.cache is None:
            self.cache = super().read()
        return self.cache

    def write(self, data):
        self.cache = data
        self._cache_modified_count += 1
        if self._cache_modified_count >= self.WRITE_CACHE_SIZE:
            self.flush()

    def flush(self):
        if self._cache_modified_count > 0:
            super().write(self.cache)
            self._cache_modified_count = 0

    def close(self):
        self.flush()
        super().close()


class LoggingMiddleware(Middleware):
    """
    Add logging capabilities to TinyDB operations.

    This Middleware logs read and write operations to a specified log file or console.
    """

    def __init__(self, storage_cls, log_file=None):
        super().__init__(storage_cls)
        self.log_file = log_file if log_file else 'tinydb.log'

    def read(self):
        self._log_operation('Read operation')
        return super().read()

    def write(self, data):
        self._log_operation('Write operation')
        super().write(data)

    def close(self):
        self._log_operation('Close operation')
        super().close()

    def _log_operation(self, operation):
        with open(self.log_file, 'a') as f:
            f.write(f"{operation} performed at {datetime.now()}\n")
