"""
Session context management.

"""
from contextlib import contextmanager
from functools import wraps

from sqlalchemy.orm import scoped_session

from microcosm_postgres.operations import new_session, recreate_all


class SessionContextFactory:
    def __init__(self) -> None:
        self.session_cls = None
        self.expire_on_commit = False

    @property
    def session(self):
        assert self.session_cls is not None, "Session not initialized"
        return self.session_cls()

    def __call__(self, graph, expire_on_commit=False):
        self.session_cls = scoped_session(
            lambda: new_session(graph, expire_on_commit)
        )
        self.expire_on_commit = expire_on_commit
        return Context(graph, self.session_cls)

    def make(self, graph, expire_on_commit=False):
        return self(graph).open()


class Context:
    def __init__(self, graph, session_cls):
        self.graph = graph
        self.session_cls = session_cls

    def open(self):
        self.session = self.session_cls()
        return self

    def close(self):
        self.session_cls.remove()

    def recreate_all(self):
        """
        Recreate all database tables, but only in a testing context.
        """
        if self.graph.metadata.testing:
            recreate_all(self.graph)

    def __enter__(self):
        return self.open()

    def __exit__(self, *args, **kwargs):
        self.close()


SessionContext = SessionContextFactory()


@contextmanager
def transaction(commit=True):
    """
    Wrap a context with a commit/rollback.

    """
    try:
        yield SessionContext.session
        if commit:
            SessionContext.session.commit()
    except Exception:
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


def maybe_transactional(func):
    """
    Variant of `transactional` that will not commit if there's an argument `commit` with a falsey value.

    Useful for dry-run style operations.

    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        commit = kwargs.get("commit", True)
        with transaction(commit=commit):
            return func(*args, **kwargs)
    return wrapper
