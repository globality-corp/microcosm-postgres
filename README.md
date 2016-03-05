# microcosm-postgres

Opinionated persistence with PostgreSQL.


## Conventions

Basics:

 -  Databases are segmented by microservice; no service can see another's database
 -  Every microservice connects to its database with a username and a password
 -  Unit testing uses an real (non-mock) database with a non-overlapping name
 -  Database names and usernames are generated according to convention

Models:

 -  Persistent models use a `SQLAlchemy` declarative base class
 -  Persistent operations pass through a unifying `Store` layer
 -  Persistent operations favor explicit queries and deletes over automatic relations and cascades


## Configuration

To change the database host:

    config.postgres.host = "myhost"

To change the database password:

    config.postgres.password = "mysecretpassword"


## Test Setup

Tests (and automated builds) act as the "example" microservice and need a cooresponding database
and user:

    createuser example
    createdb -O example example_test_db

Note that production usage should always create the user with a password. For example:

    echo "CREATE ROLE example WITH LOGIN ENCRYPTED PASSWORD 'secret';" | psql

Automated test do not enforce that a password is set because many development environments
(OSX, Circle CI) configure `pg_hba.conf` for trusted login from localhost.


## TODO

 - Integrate CLI for basic operations
 - Integrate alembic migration support
