import logging
import argparse
import os

import pylo
import click
from pylo.API.CredentialsManager import get_all_credentials, create_credential_in_file, CredentialFileEntry, \
    create_credential_in_default_file

from pylo import log
from . import Command


command_name = "cred-manager"
objects_load_filter = None


def fill_parser(parser: argparse.ArgumentParser):
    sub_parser = parser.add_subparsers(dest='sub_command', required=True)
    list_parser = sub_parser.add_parser('list', help='List all credentials')
    create_parser = sub_parser.add_parser('create', help='Create a new credential')

    create_parser.add_argument('--name', required=True, type=str,
                               help='Name of the credential')
    create_parser.add_argument('--fqdn', required=True, type=str,
                                 help='FQDN of the PCE')
    create_parser.add_argument('--port', required=True, type=int,
                                 help='Port of the PCE')
    create_parser.add_argument('--org', required=True, type=int,
                                 help='Organization ID')
    create_parser.add_argument('--api-user', required=True, type=str,
                                    help='API user')
    create_parser.add_argument('--verify-ssl', required=True, type=bool,
                                 help='Verify SSL')


def __main(args, **kwargs):
    if args['sub_command'] == 'list':
        credentials = get_all_credentials()
        # sort credentials by name
        credentials.sort(key=lambda x: x.name)

        # print credentials in a nice table
        table_template = " {:<19} {:<40} {:<22} {:<25}"
        print(table_template.format("Name", "URL", "API User", "Originating File"))

        for credential in credentials:
            print(table_template.format(credential.name,
                                        credential.hostname + ':' + str(credential.port),
                                        credential.api_user,
                                        credential.originating_file)
                  )

    elif args['sub_command'] == 'create':
        print("Recap:")
        print("Name: {}".format(args['name']))
        print("FQDN: {}".format(args['fqdn']))
        print("Port: {}".format(args['port']))
        print("Org ID: {}".format(args['org']))
        print("API User: {}".format(args['api_user']))
        print("Verify SSL: {}".format(args['verify_ssl']))
        print()

        print("* Checking if a credential with the same name already exists...", flush=True, end="")
        credentials = get_all_credentials()
        for credential in credentials:
            if credential.name == args['name']:
                raise pylo.PyloEx("A credential named '{}' already exists".format(args['name']))
        print("OK!")

        # prompt of API key from user input, single line, hidden
        api_key = click.prompt('> API Key', hide_input=True)

        credentials_data: CredentialFileEntry = {
            "name": args['name'],
            "hostname": args['fqdn'],
            "port": args['port'],
            "org_id": args['org'],
            "api_user": args['api_user'],
            "verify_ssl": args['verify_ssl'],
            "api_key": api_key
        }

        cwd = os.getcwd()
        create_in_current_workdir = click.prompt('> Create in current workdir? Y/N ({})'.format(cwd), type=bool)


        print("* Creating credential...", flush=True, end="")
        if create_in_current_workdir:
            create_credential_in_file(file_full_path=cwd, data=credentials_data)
        else:
            create_credential_in_default_file(data=credentials_data)

        print("OK!")









command_object = Command(command_name, __main, fill_parser, credentials_manager_mode=True)

