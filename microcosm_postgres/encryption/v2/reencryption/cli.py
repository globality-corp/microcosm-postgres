from argparse import ArgumentParser


class ReencryptionCli:

    def __init__(self):
        self.parser = ArgumentParser(description="Reenecrption CLI")
        self.subparsers = self.parser.add_subparsers()

    def add_reencrypt_command(self, fn):
        reencrypt_command_parser = self.subparsers.add_parser('reencrypt', help='Reencrypt some data')
        reencrypt_command_parser.add_argument("--client-id", help="The client id to reencrypt")
        reencrypt_command_parser.set_defaults(func=fn)

    def add_audit_command(self, fn):
        audit_command_parser = self.subparsers.add_parser('audit', help='Audit some data')
        audit_command_parser.set_defaults(func=fn)

    def __call__(self, *args, **kwargs):
        # Parse arguments
        args = self.parser.parse_args()

        # If no command is provided, display help
        if not hasattr(args, 'func'):
            self.parser.print_help()
            return

        # Call the function associated with the chosen subcommand
        args.func(args)
