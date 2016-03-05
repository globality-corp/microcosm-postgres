# microcosm-postgres

Opinionated persistence with PostgreSQL.


## Conventions

 -  Databases are segmented by microservice; no service can see another's database
 -  Every microservice connects to its database with a username and password (always)
 -  Unit testing uses an real (non-mock) database with a non-overlapping name
 -  Persistent models use a `SQLAlchemy` declarative base class
 -  Persistent operations pass through a unifying `Store` layer
 -  Persistent operations favor explicit relationship lookups and cascades


## Configuration

To change the database host:

    config.postgres.host = "myhost"

To change the database password:

    config.postgres.password = "mysecretpassword"


## Setup

Initial setup:

    createuser test
    createdb -O test test_db
    createdb -O test test_test_db


## TODO

 - Tune database connections (pools, timeouts, check on borrow, echo)
 - Test password validation failures
 - Integrate CLI for basic operations
 - Integrate alembic migration support
