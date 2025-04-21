from __future__ import annotations

import contextlib
try:
    import fcntl
except ImportError:
    pass
import typing as t


@contextlib.contextmanager
def named_mutex(path: str) -> t.Iterator[None]:
    """
    Lightweight context manager wrapper over `fcntl.flock` to provide IPC locking via a shared filename.
    Entering the context manager blocks until the lock is acquired.
    The lock file will be created automatically, but creation of the parent directory and deletion of the lockfile are the caller's responsibility.
    """
    with open(path, 'a') as file:
        fcntl.flock(file, fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(file, fcntl.LOCK_UN)

@contextlib.contextmanager
def no_mutex(path: str) -> t.Iterator[None]:
    yield