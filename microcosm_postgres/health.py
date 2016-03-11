"""
Simple Postgres health check.

"""
from microcosm_postgres.context import Context


def check_health(graph):
    Context.session.execute("SELECT 1;")
