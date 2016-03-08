"""
Session context management.

"""
from contextlib import contextmanager
from functools import wraps

from microcosm_postgres.operations import create_all, drop_all, new_session


class Context(object):
    """
    Save current session in well-known location and provide context management.

    """
    session = None

    def __init__(self, graph, expire_on_commit=False):
        self.graph = graph
        self.expire_on_commit = expire_on_commit

    def open(self):
        Context.session = new_session(self.graph, self.expire_on_commit)
        return self

    def close(self):
        Context.session.close()
        Context.session = None

    def recreate_all(self):
        """
        Recreate all database tables, but only in a testing context.
        """
        if self.graph.metadata.testing:
            drop_all(self.graph)
            create_all(self.graph)

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
        yield Context.session
        Context.session.commit()
    except:
        Context.session.rollback()
        raise


def with_transaction(func):
    """
    Decorate a function call with a commit/rollback and pass the session as the first arg.

    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        with transaction() as session:
            return func(session, *args, **kwargs)
    return wrapper
