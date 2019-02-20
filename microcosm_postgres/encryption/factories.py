from microcosm_postgres.encryption.models import EncryptableMixin


def configure_encryptor(graph):
    """
    Create a MultiTenantEncryptor from configured keys.

    """
    encryptor = graph.multi_tenant_key_registry.make_encryptor(graph)

    # register the encryptor will each encryptable type
    for encryptable in EncryptableMixin.__subclasses__():
        encryptable.register(encryptor)

    return encryptor
