"""
Contains some utils around session contexts.

"""
import contextlib
from typing import Any, Generator

from microcosm.object_graph import ObjectGraph

from microcosm_postgres.context import SessionContext
from microcosm_postgres.encryption.v2.encryptors import AwsKmsEncryptor


@contextlib.contextmanager
def encryptor_context_as_client(graph: ObjectGraph, client_id: str) -> Generator[None, Any, None]:
    """Manually choose the client_id you would like to choose."""
    encryptor = graph.multi_tenant_encryptor[client_id]
    with AwsKmsEncryptor.set_encryptor_context(client_id, encryptor):
        yield


@contextlib.contextmanager
def encryptor_session_context_as_client(graph: ObjectGraph, client_id: str) -> Generator[None, Any, None]:
    with SessionContext(graph), encryptor_context_as_client(graph, client_id):
        yield
