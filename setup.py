#!/usr/bin/env python
from setuptools import find_packages, setup

project = "microcosm-postgres"
version = "1.1.0"

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
        "alembic>=0.9.6",
        "microcosm>=2.0.0",
        "psycopg2-binary>=2.7.4",
        "python_dateutil>=2.6.1",
        "pytz>=2017.3",
        "SQLAlchemy>=1.2.0",
        "SQLAlchemy-Utils>=0.32.21",
    ],
    setup_requires=[
        "nose>=1.3.6",
    ],
    dependency_links=[
    ],
    entry_points={
        "microcosm.factories": [
            "sessionmaker = microcosm_postgres.factories:configure_sqlalchemy_sessionmaker",
        ],
    },
    tests_require=[
        "coverage>=3.7.1",
        "enum34>=1.1.6",
        "mock>=1.0.1",
        "PyHamcrest>=1.8.5",
    ],
)
