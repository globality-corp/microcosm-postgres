#!/usr/bin/env python
from setuptools import find_packages, setup


project = "microcosm-postgres"
version = "3.11.0"

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
    python_requires=">=3.9",
    keywords="microcosm",
    install_requires=[
        "alembic>=1.0.0",
        "microcosm>=2.12.0",
        "microcosm-logging>=1.5.0",
        "psycopg2-binary>=2.7.5",
        "python-dateutil>=2.7.3",
        "pytz>=2018.5",
        "SQLAlchemy>=2.0.0",
        "SQLAlchemy-Utils>=0.37.0",
    ],
    setup_requires=[
    ],
    dependency_links=[
    ],
    extras_require={
        "metrics": "microcosm-metrics>=2.5.0",
        "encryption": [
            "aws-encryption-sdk>=2.0.0",
            "cryptography>=35",
        ],
        "test": [
            "aws-encryption-sdk>=2.0.0",
            "coverage>=3.7.1",
            "PyHamcrest>=1.8.5",
            "pytest-cov>=3.0.0",
            "pytest>=6.2.5",
        ],
    },
    entry_points={
        "microcosm.factories": [
            "default_engine_routing_strategy = microcosm_postgres.factories.engine_routing_strategy:DefaultEngineRoutingStrategy",  # noqa: E501
            "model_engine_routing_strategy = microcosm_postgres.factories.engine_routing_strategy:ModelEngineRoutingStrategy",  # noqa: E501
            "multi_tenant_encryptor = microcosm_postgres.encryption.factories:configure_encryptor [encryption]",
            "multi_tenant_key_registry = microcosm_postgres.encryption.registry:MultiTenantKeyRegistry [encryption]",
            "materials_manager = microcosm_postgres.encryption.providers:configure_materials_manager [encryption]",
            "postgres = microcosm_postgres.factories.engine:configure_engine",
            "postgres_store_metrics = microcosm_postgres.metrics:PostgresStoreMetrics",
            "sessionmaker = microcosm_postgres.factories.sessionmaker:configure_sessionmaker",
            "sessionmakers = microcosm_postgres.factories.sessionmakers:configure_sessionmakers",
            "shards = microcosm_postgres.factories.shards:configure_shards",
            "register_flask_context = microcosm_postgres.encryption.v2.encryptors:AwsKmsEncryptor.register_flask_context [encryption]",  # noqa: E501
        ],
    },
)
