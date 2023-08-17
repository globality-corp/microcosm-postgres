"""
Build an Engine for each horizontal shard we define.

By default, we use the "postgres" engine as the only shard called "global".
"""
import json
from typing import Any, Dict

from microcosm.api import binding, defaults
from microcosm.config.model import Configuration
from microcosm.config.types import boolean
from microcosm.config.validation import typed, validate
from microcosm.metadata import Metadata
from sqlalchemy.ext.horizontal_shard import ShardedSession
from sqlalchemy.orm.session import sessionmaker

from microcosm_postgres.constants import (
    GLOBAL_SHARD_NAME,
    X_REQUEST_CLIENT_HEADER,
    X_REQUEST_SHARD_HEADER,
)
from microcosm_postgres.factories.engine import make_engine


def load_shard_defaults(config: Configuration, metadata: Metadata) -> None:
    requirements = Configuration(
        # database name will be chosen automatically if not supplied
        database_name=None,
        # database driver name; default should suffice
        driver="postgresql",
        # enable SQL echoing (verbose; only use for narrow debugging)
        echo=typed(boolean, default_value=False),
        # connection host; will usually need to be overridden (except for local dev)
        host="localhost",
        # the number of extra connections over/above the pool size; 10 is the default
        max_overflow=typed(int, default_value=10),
        # password; will usually need to be overridden (except for local development)
        password="",
        # the default size of the connection pool
        pool_size=typed(int, default_value=5),
        # the timeout waiting for a connection from the pool; default 30 is too large
        pool_timeout=typed(int, default_value=2),
        # recycle connections in pool after given number of seconds; -1 means no recycle
        pool_recycle=typed(int, default_value=-1),
        # enable the connection pool 'pre-ping' to test connections for liveness
        # upon each checkout
        pool_pre_ping=typed(boolean, default_value=False),
        # the postgres connection port
        port=typed(int, default_value=5432),
        # enable read_only username convention; depends on out-of-band configuration
        read_only=typed(boolean, default_value=False),
        # use SSL to connect to postgres?
        require_ssl=typed(boolean, default_value=False),
        # username will be chosen automatically if not supplied
        username=None,
        # verify SSL connections using certificates? (requires `ssl_cert_path`)
        verify_ssl=typed(boolean, default_value=False),
        # SSL certificate path (used with `verify_ssl`)
        ssl_cert_path=None,
        # Engine retries if disconnects occur
        engine_retries=typed(int, default_value=10),
        # Engine retry interval in milliseconds
        engine_retry_interval=typed(int, default_value=100),
    )
    for k, v in requirements.items():
        if k not in config:
            config[k] = v
    validate(requirements, metadata, config)


@binding("client_shard")
@defaults(mapping=typed(str, default_value="{}"))
def configure_client_shard_map(graph):
    return json.loads(graph.config.client_shard.mapping)


@binding("shards")
def configure_shards(graph):
    if not graph.config.shards:
        return {GLOBAL_SHARD_NAME: graph.postgres.engine}

    for shard in graph.config.shards.values():
        load_shard_defaults(shard.postgres, graph.metadata)

    return {
        name: make_engine(graph.metadata, shard)
        for name, shard in graph.config.shards.items()
    }


def configure_sharded_sessionmaker(graph):
    """
    Create the SQLAlchemy sharded sessionmaker.

    The session maker is sharded based on the `x-request-shard` or the`x-request-client`
    header. If both are present the `x-request-shard` takes precedence default `global`.

    To use sharded sessionmaker, in your app bind this function as the `sessionmaker`

    ```python
    binding(sessionmaker)(configure_sharded_sessionmaker)
    ```
    """

    def normalise(opaque: Dict[str, Any]) -> Dict[str, Any]:
        return {k.lower(): v for k, v in opaque.items()}

    def select_shard():
        context = normalise(graph.opaque)
        if shard_name := context.get(X_REQUEST_SHARD_HEADER):
            return shard_name

        if client_id := context.get(X_REQUEST_CLIENT_HEADER):
            return graph.client_shard.get(client_id, GLOBAL_SHARD_NAME)
        return GLOBAL_SHARD_NAME

    def shard_chooser(mapper, instance, clause=None):
        """Choose which shard to send the object instance to for mutations"""
        return select_shard()

    def execute_chooser(context):
        """Choose which shard to query."""
        return [select_shard()]

    def id_chooser(query, ident):
        """Choose which shard to select the server generated id from."""
        return [select_shard()]

    sm = sessionmaker(
        class_=ShardedSession,
        shards=graph.shards,
    )
    sm.configure(
        execute_chooser=execute_chooser,
        shard_chooser=shard_chooser,
        id_chooser=id_chooser,
    )
    return sm
