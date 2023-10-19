import io
import sys
from contextlib import redirect_stdout

import pytest

from microcosm_postgres.encryption.v2.reencryption.cli import ReencryptionCli


def mock_reencrypt_fn(args):
    assert args.client_id == "123"
    return "Reencryption successful!"


def mock_audit_fn(args):
    return "Audit completed!"


cli = ReencryptionCli(mock_reencrypt_fn, mock_audit_fn)


# Helper function to check if an exception is raised
def raises_help_text(func, expected_exit_code, *args, **kwargs):
    with pytest.raises(SystemExit) as e:
        func(*args, **kwargs)
    assert e.value.code == expected_exit_code


def test_reencrypt_command_setup():
    sys.argv = ["prog_name", "reencrypt", "--client-id", "123"]
    cli()


def test_audit_command_setup():
    sys.argv = ["prog_name", "audit"]
    cli()


def test_no_command_displays_help():
    sys.argv = ["prog_name"]

    # Capture stdout
    with io.StringIO() as buf, redirect_stdout(buf):
        cli()
        output = buf.getvalue()

    assert "Reenecrption CLI" in output  # or any other part of your expected help message


def test_invalid_command_displays_help():
    sys.argv = ["prog_name", "invalid_command"]
    raises_help_text(cli, 2)  # We expect an exit code of 2 here
