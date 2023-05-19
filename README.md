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

Tests (and automated builds) act as the "example" microservice and need a corresponding database
and user:

    createuser example
    createdb -O example example_test_db

Note that production usage should always create the user with a password. For example:

    echo "CREATE ROLE example WITH LOGIN ENCRYPTED PASSWORD 'secret';" | psql

Automated test do not enforce that a password is set because many development environments
(OSX, Circle CI) configure `pg_hba.conf` for trusted login from localhost.

## Migration guide: 1.x => 2.x

### Encryption

If you are updating from microcosm-postgres 1.x to 2.x and use encryption, there are a couple of
configuration updates you will need to add in order to keep encryption working as expected.

* the `multi_tenant_key_registry` has two new parameters to account for `aws-encryption-sdk` updates
* `account_ids` should be set to the list of AWS account IDs that contain the keys being using for encryption
* `partitions` should be the list of AWS partitions used to access your encryption keys. This should be a single
item per registry entry

General configuration will look like the existing configuration used for the registry (e.g. using `;` as a delimiter
between items per registry entry).

Full configuration may thus look something like this:

    multi_tenant_key_registry=dict(
         context_keys="client-id1,client-id2",
         key_ids="key1a;key1b,key2a;key2b",
         partitions="aws,aws-cn",
         account_ids="account-id1a;account-id1b,account-id2",
    )

For more information, see the "Discovery Mode" section in:
https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/migrate-mkps-v2.html


## Migration guide: 2.x => 3.x
Version 3.x uses sqlalchemy 2.x, any breaking changes are introduced by sqlalchemy 2.

The official [sqlalchemy 2 migration guide](https://docs.sqlalchemy.org/en/20/changelog/migration_20.html) contains step to ensure your application is compatible with sqlalchemy 2.

Additionally, follow https://docs.sqlalchemy.org/en/20/changelog/whatsnew_20.html#orm-declarative-models to update your models to use the new declarative styles.
