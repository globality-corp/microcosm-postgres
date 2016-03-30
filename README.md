# microcosm-postgres

Opinionated persistence with PostgreSQL.


[![Circle CI](https://circleci.com/gh/globality-corp/microcosm-postgres/tree/develop.svg?style=svg)](https://circleci.com/gh/globality-corp/microcosm-postgres/tree/develop)


## Usage

This project includes example models and persistence stores. Assuming the testing
database exists (see below), the following demonstrates basic usage:

    from microcosm.api import create_object_graph
    from microcosm_postgres.context import SessionContext, transaction
    from microcosm_postgres.example import Company

    # create the object graph
    graph = create_object_graph(name="example", testing=True)

    # wire up the persistence layer to the (testing) database
    [company_store] = graph.use("company_store")

    # set up a session
    with SessionContext(graph) as context:

        # drop and create database tables; *only* do this for testing
        context.recreate_all()

        with transaction():
            # create a model
            company = company_store.create(Company(name="Acme"))

        # prints 1
        print company_store.count()


## Convention

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
