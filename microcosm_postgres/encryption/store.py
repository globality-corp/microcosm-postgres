from microcosm_postgres.store import Store


class EncryptableStore(Store):
    """
    A store for (conditionally) encryptable model.

    The store supports delete action for encryptable models by deleting
    the encrypted model.
    Note: in order to use the store, the model must define:
    -  An `encrypted_identifier` property (defaults to `self.encrypted_id`)

    """

    def __init__(self, graph, model_class, encrypted_store, **kwargs):
        super().__init__(graph, model_class, **kwargs)
        self.encrypted_store = encrypted_store

    def delete(self, identifier):
        instance = self.retrieve(identifier)
        result = super().delete(identifier)
        if instance.encrypted_identifier:
            self.encrypted_store.delete(instance.encrypted_identifier)
        return result
