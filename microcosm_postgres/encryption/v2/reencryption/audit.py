from microcosm_postgres.encryption.v2.reencryption.utils import find_models_using_encryption, \
    print_reencryption_usage_info


def run_audit():
    """Show an overview of the Tables and Columns which appear to be leveraging encryption."""
    models = find_models_using_encryption()
    print_reencryption_usage_info(models)
