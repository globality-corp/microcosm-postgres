"""
Simple Postgres health check.

"""
from microcosm_postgres.context import SessionContext


def check_health(graph):
    SessionContext.session.execute("SELECT 1;")
