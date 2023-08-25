"""
Build an Engine for each horizontal shard we define.

By default, we use the "postgres" engine as the only shard called "global".
"""
import json
from typing import Any, Dict

from microcosm.api import binding, defaults
from microcosm.config.model import Configuration
from microcosm.config.types import boolean
from microcosm.config.validation import required, typed, validate
from microcosm.errors import ValidationError
from microcosm.metadata import Metadata
from sqlalchemy.orm import Session, sessionmaker

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
        host=required(str),
        # the number of extra connections over/above the pool size; 10 is the default
        max_overflow=typed(int, default_value=10),
        # password; will usually need to be overridden (except for local development)
        password=required(str),
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


def connect_shards(graph):
    valid_shards = []

    for name, shard in graph.config.shards.items():
        try:
            load_shard_defaults(shard.postgres, graph.metadata)
        except ValidationError:
            graph.logger.warning(
                "Configuration for {shard} is incomplete",
                extra=dict(shard=shard),
            )
            continue
        valid_shards.append(name)

    return {
        name: make_engine(graph.metadata, graph.config.shards[name])
        for name in valid_shards
    }


@binding("shards")
def configure_shards(graph):
    shards = connect_shards(graph)

    if GLOBAL_SHARD_NAME not in shards:
        graph.logger.warning(
            "Global shard not explicitly defined, using single shard postgres"
        )
        return {GLOBAL_SHARD_NAME: graph.postgres.engine}

    return shards


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

    graph.use(
        "client_shard",
        "sessionmakers",
    )  # Automatically bind shard related resources

    def normalise(opaque: Dict[str, Any]) -> Dict[str, Any]:
        return {k.lower(): v for k, v in opaque.items()}

    def select_shard():
        context = normalise(graph.opaque)
        if shard_name := context.get(X_REQUEST_SHARD_HEADER):
            return shard_name

        if client_id := context.get(X_REQUEST_CLIENT_HEADER):
            return graph.client_shard.get(client_id, GLOBAL_SHARD_NAME)
        return GLOBAL_SHARD_NAME

    class SingleShardedSession(Session):
        """
        A session that always uses the same shard.

        This is useful for read-only sessions.
        """

        def get_bind(self, *args, **kwargs):
            return graph.shards[select_shard()]

    return sessionmaker(class_=SingleShardedSession)
