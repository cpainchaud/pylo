from prettytable import PrettyTable
import argparse
import os

import paramiko

import illumio_pylo as pylo
import click
from illumio_pylo.API.CredentialsManager import get_all_credentials, create_credential_in_file, CredentialFileEntry, \
    create_credential_in_default_file, encrypt_api_key_with_paramiko_key, decrypt_api_key_with_paramiko_key

from illumio_pylo import log
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
                                        credential.fqdn + ':' + str(credential.port),
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
            "fqdn": args['fqdn'],
            "port": args['port'],
            "org_id": args['org'],
            "api_user": args['api_user'],
            "verify_ssl": args['verify_ssl'],
            "api_key": api_key
        }

        encrypt_api_key = click.prompt('> Encrypt API? Y/N', type=bool)
        if encrypt_api_key:
            print("Available keys (ECDSA NISTPXXX keys and a few others are not supported and will be filtered out):")
            ssh_keys = paramiko.Agent().get_keys()
            # filter out ECDSA NISTPXXX and sk-ssh-ed25519@openssh.com
            ssh_keys = get_supported_keys_from_ssh_agent()

            # display a table of keys
            print_keys(keys=ssh_keys, display_index=True)
            print()

            index_of_selected_key = click.prompt('> Select key by ID#', type=click.IntRange(0, len(ssh_keys)-1))
            selected_ssh_key = ssh_keys[index_of_selected_key]
            print("Selected key: {} | {} | {}".format(selected_ssh_key.get_name(),
                                                      selected_ssh_key.get_fingerprint().hex(),
                                                      selected_ssh_key.comment))
            print(" * encrypting API key with selected key (you may be prompted by your SSH agent for confirmation or PIN code) ...", flush=True, end="")
            encrypted_api_key = encrypt_api_key_with_paramiko_key(ssh_key=selected_ssh_key, api_key=api_key)
            print("OK!")
            print(" * trying to decrypt the encrypted API key...", flush=True, end="")
            decrypted_api_key = decrypt_api_key_with_paramiko_key(encrypted_api_key_payload=encrypted_api_key)
            if decrypted_api_key != api_key:
                raise pylo.PyloEx("Decrypted API key does not match original API key")
            print("OK!")
            credentials_data["api_key"] = encrypted_api_key


        cwd = os.getcwd()
        create_in_current_workdir = click.prompt('> Create in current workdir ({})? If not then user homedir will be used.   Y/N '.format(cwd), type=bool)


        print("* Creating credential...", flush=True, end="")
        if create_in_current_workdir:
            file_path = create_credential_in_file(file_full_path=cwd, data=credentials_data)
        else:
            file_path = create_credential_in_default_file(data=credentials_data)

        print("OK! ({})".format(file_path))


command_object = Command(command_name, __main, fill_parser, credentials_manager_mode=True)


def get_supported_keys_from_ssh_agent() -> list[paramiko.AgentKey]:
    keys = paramiko.Agent().get_keys()
    # filter out ECDSA NISTPXXX and sk-ssh-ed25519
    # RSA and ED25519 keys are reported to be working
    return [key for key in keys if not (key.get_name().startswith("ecdsa-sha2-nistp") or
                                        key.get_name().startswith("sk-ssh-ed25519"))]

def print_keys(keys: list[paramiko.AgentKey], display_index = True) -> None:

    args_for_print = []

    column_properties = [  # (name, width)
        ("ID#", 4),
        ("Type", 20),
        ("Fingerprint", 40),
        ("Comment", 48)
    ]

    if not display_index:
        # remove tuple with name "ID#"
        column_properties = [item for item in column_properties if item[0] != "ID#"]


    table = PrettyTable()
    table.field_names = [item[0] for item in column_properties]


    for i, key in enumerate(keys):
        display_values = []
        if display_index:
            display_values.append(i)
        display_values.append(key.get_name())
        display_values.append(key.get_fingerprint().hex())
        display_values.append(key.comment)

        table.add_row(display_values)

    print(table)


