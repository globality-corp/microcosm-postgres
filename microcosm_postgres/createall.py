"""
Create databases.

"""
from argparse import ArgumentParser

from microcosm_postgres.operations import create_all, drop_all


def parse_args(graph):
    parser = ArgumentParser()
    parser.add_argument("--drop", "-D", action="store_true")
    return parser.parse_args()


def main(graph):
    """
    Create and drop databases.

    """
    args = parse_args(graph)

    if args.drop:
        drop_all(graph)
    create_all(graph)
