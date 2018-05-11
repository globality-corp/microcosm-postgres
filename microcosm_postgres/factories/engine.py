"""
Create a SQL alchemy postgres engine.

"""
from microcosm.api import binding, defaults
from microcosm.config.validation import typed
from microcosm.config.types import boolean
from sqlalchemy import create_engine


def choose_database_name(metadata, config):
    """
    Choose the database name to use.

    As a default, databases should be named after the service that uses them. In addition,
    database names should be different between unit testing and runtime so that there is
    no chance of a unit test dropping a real database by accident.

    """
    if config.database_name is not None:
        # we allow -- but do not encourage -- database name configuration
        return config.database_name

    if metadata.testing:
        # by convention, we provision different databases for unit testing and runtime
        return f"{metadata.name}_test_db"

    return f"{metadata.name}_db"


def choose_username(metadata, config):
    """
    Choose the database username to use.

    Because databases should not be shared between services, database usernames should be
    the same as the service that uses them.

    """
    if config.username is not None:
        # we allow -- but do not encourage -- database username configuration
        return config.username

    if config.read_only:
        # by convention, we provision read-only username for every service
        return f"{metadata.name}_ro"

    return metadata.name


def choose_uri(metadata, config):
    """
    Choose the database URI to use.

    """
    database_name = choose_database_name(metadata, config)
    driver = config.driver
    host, port = config.host, config.port
    username, password = choose_username(metadata, config), config.password

    return f"{driver}://{username}:{password}@{host}:{port}/{database_name}"


def choose_connect_args(metadata, config):
    """
    Choose the SSL mode and optional root cert for the connection.

    """
    if not config.require_ssl and not config.verify_ssl:
        return dict(
            sslmode="prefer",
        )

    if config.require_ssl and not config.verify_ssl:
        return dict(
            sslmode="require",
        )

    if not config.ssl_cert_path:
        raise Exception("SSL certificate path (`ssl_cert_path`) must be configured for verification")

    return dict(
        sslmode="verify-full",
        sslrootcert=config.ssl_cert_path,
    )


def choose_args(metadata, config):
    """
    Choose database connection arguments.

    """
    return dict(
        connect_args=choose_connect_args(metadata, config),
        echo=config.echo,
        max_overflow=config.max_overflow,
        pool_size=config.pool_size,
        pool_timeout=config.pool_timeout,
    )


def make_engine(metadata, config):
    uri = choose_uri(metadata, config.postgres)
    args = choose_args(metadata, config.postgres)
    return create_engine(
        uri,
        **args,
    )


@binding("postgres")
@defaults(
    # database name will be chosen automatically if not supplied
    database_name=None,
    # database driver name; default should suffice
    driver="postgresql",
    # enable SQL echoing (verbose; only use for narrow debugging)
    echo=typed(boolean, default_value=False),
    # connection host; will usually need to be overriden (except for local development)
    host="localhost",
    # the number of extra connections over/above the pool size; 10 is the default
    max_overflow=typed(int, default_value=10),
    # password; will usually need to be overriden (except for local development)
    password="",
    # the default size of the connection pool
    pool_size=typed(int, default_value=5),
    # the timeout waiting for a connection from the pool; 30 is the default and much too large
    pool_timeout=typed(int, default_value=2),
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
)
def configure_engine(graph):
    return make_engine(graph.metadata, graph.config)
