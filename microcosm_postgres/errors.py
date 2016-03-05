"""
Persistence errors.

Errors define a `status_code` for each translation to HTTP (because REST)
but are not coupled with any HTTP library.

"""


class ModelIntegrityError(Exception):
    """
    An attempt to create or update a model violated a schema constraint.

    Usually the result of a programming error.

    """
    @property
    def status_code(self):
        # internal server error
        return 500


class DuplicateModelError(ModelIntegrityError):
    """
    An attempt to create or update a module violated a uniqueness constraint.

    Unlike `ModelIntegrityError`, duplicates are often expected behavior.

    """
    @property
    def status_code(self):
        # conflict
        return 409


class ReferencedModelError(ModelIntegrityError):
    """
    An attempt to delete a model didn't first delete references.

    """
    @property
    def status_code(self):
        # forbidden
        return 403


class ModelNotFoundError(Exception):
    """
    The queried or updated model did not exist.

    """
    @property
    def status_code(self):
        # not found
        return 404
