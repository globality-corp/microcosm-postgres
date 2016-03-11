"""
Session context management.

"""
from contextlib import contextmanager
from functools import wraps

from microcosm_postgres.operations import new_session, recreate_all


class SessionContext(object):
    """
    Save current session in well-known location and provide context management.

    """
    session = None

    def __init__(self, graph, expire_on_commit=False):
        self.graph = graph
        self.expire_on_commit = expire_on_commit

    def open(self):
        SessionContext.session = new_session(self.graph, self.expire_on_commit)
        return self

    def close(self):
        if SessionContext.session:
            SessionContext.session.close()
            SessionContext.session = None

    def recreate_all(self):
        """
        Recreate all database tables, but only in a testing context.
        """
        if self.graph.metadata.testing:
            recreate_all(self.graph)

    @classmethod
    def make(cls, graph, expire_on_commit=False):
        """
        Create an opened context.

        """
        return cls(graph, expire_on_commit).open()

    # context manager

    def __enter__(self):
        return self.open()

    def __exit__(self, *args, **kwargs):
        self.close()


@contextmanager
def transaction():
    """
    Wrap a context with a commit/rollback.

    """
    try:
        yield SessionContext.session
        SessionContext.session.commit()
    except:
        if SessionContext.session:
            SessionContext.session.rollback()
        raise


def transactional(func):
    """
    Decorate a function call with a commit/rollback and pass the session as the first arg.

    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        with transaction():
            return func(*args, **kwargs)
    return wrapper
