#!/usr/bin/env python
from setuptools import find_packages, setup

project = "microcosm-postgres"
version = "1.5.0"

setup(
    name=project,
    version=version,
    description="Opinionated persistence with PostgreSQL",
    author="Globality Engineering",
    author_email="engineering@globality.com",
    url="https://github.com/globality-corp/microcosm-postgres",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    include_package_data=True,
    zip_safe=False,
    keywords="microcosm",
    install_requires=[
        "alembic>=0.9.9",
        "microcosm>=2.4.0",
        "psycopg2-binary>=2.7.4",
        "python-dateutil>=2.7.3",
        "pytz>=2018.4",
        "SQLAlchemy>=1.2.7",
        "SQLAlchemy-Utils>=0.33.3",
    ],
    setup_requires=[
        "nose>=1.3.6",
    ],
    dependency_links=[
    ],
    entry_points={
        "microcosm.factories": [
            "default_engine_routing_strategy = microcosm_postgres.factories.engine_routing_strategy:DefaultEngineRoutingStrategy",  # noqa: E501
            "model_engine_routing_strategy = microcosm_postgres.factories.engine_routing_strategy:ModelEngineRoutingStrategy",  # noqa: E501
            "postgres = microcosm_postgres.factories.engine:configure_engine",
            "sessionmaker = microcosm_postgres.factories.sessionmaker:configure_sessionmaker",
        ],
    },
    tests_require=[
        "coverage>=3.7.1",
        "enum34>=1.1.6",
        "mock>=1.0.1",
        "PyHamcrest>=1.8.5",
    ],
)
