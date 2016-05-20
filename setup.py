#!/usr/bin/env python
from setuptools import find_packages, setup

project = "microcosm-postgres"
version = "0.13.0"

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
        "alembic>=0.8.4",
        "microcosm>=0.9.0",
        "psycopg2>=2.6.1",
        "python_dateutil>=2.5.0",
        "pytz>=2016.3",
        "sqlalchemy>=1.0.12",
        "SQLAlchemy-Utils>=0.31.6",
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
        "mock>=1.0.1",
        "PyHamcrest>=1.8.5",
    ],
)
