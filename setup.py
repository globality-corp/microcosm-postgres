#!/usr/bin/env python
from setuptools import find_packages, setup

project = "microcosm-postgres"
version = "1.15.2"

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
    python_requires=">=3.6",
    keywords="microcosm",
    install_requires=[
        "alembic>=1.0.0",
        "microcosm>=2.6.0",
        "microcosm-logging>=1.5.0",
        "psycopg2-binary>=2.7.5",
        "python-dateutil>=2.7.3",
        "pytz>=2018.5",
        "SQLAlchemy>=1.2.11",
        "SQLAlchemy-Utils>=0.33.3",
    ],
    setup_requires=[
        "nose>=1.3.6",
    ],
    dependency_links=[
    ],
    extras_require={
        "encryption": "aws-encryption-sdk>=1.3.8",
    },
    entry_points={
        "microcosm.factories": [
            "default_engine_routing_strategy = microcosm_postgres.factories.engine_routing_strategy:DefaultEngineRoutingStrategy",  # noqa: E501
            "model_engine_routing_strategy = microcosm_postgres.factories.engine_routing_strategy:ModelEngineRoutingStrategy",  # noqa: E501
            "multi_tenant_encryptor = microcosm_postgres.encryption.factories:configure_encryptor [encryption]",
            "multi_tenant_key_registry = microcosm_postgres.encryption.registry:MultiTenantKeyRegistry [encryption]",
            "materials_manager = microcosm_postgres.encryption.providers:configure_materials_manager [encryption]",
            "postgres = microcosm_postgres.factories.engine:configure_engine",
            "sessionmaker = microcosm_postgres.factories.sessionmaker:configure_sessionmaker",
        ],
    },
    tests_require=[
        "coverage>=3.7.1",
        "PyHamcrest>=1.8.5",
    ],
)
