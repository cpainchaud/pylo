import sys

from prettytable import PrettyTable
import argparse
import os

import paramiko

import illumio_pylo as pylo
import click
from illumio_pylo.API.CredentialsManager import get_all_credentials, create_credential_in_file, CredentialFileEntry, \
    create_credential_in_default_file, delete_credential_from_file, \
    get_credentials_from_file, encrypt_api_key_with_paramiko_ssh_key_chacha20poly1305, \
    decrypt_api_key_with_paramiko_ssh_key_chacha20poly1305, get_supported_keys_from_ssh_agent, is_encryption_available

from . import Command


command_name = "cred-manager"
objects_load_filter = None


def fill_parser(parser: argparse.ArgumentParser):
    sub_parser = parser.add_subparsers(dest='sub_command', required=True)
    list_parser = sub_parser.add_parser('list', help='List all credentials')

    test_parser = sub_parser.add_parser('test', help='Test a credential')
    test_parser.add_argument('--name', required=False, type=str, default=None,
                             help='Name of the credential profile to test')

    create_parser = sub_parser.add_parser('create', help='Create a new credential')
    create_parser.add_argument('--name', required=False, type=str, default=None,
                               help='Name of the credential')
    create_parser.add_argument('--fqdn', required=False, type=str, default=None,
                               help='FQDN of the PCE')
    create_parser.add_argument('--port', required=False, type=int, default=None,
                               help='Port of the PCE')
    create_parser.add_argument('--org', required=False, type=int, default=None,
                               help='Organization ID')
    create_parser.add_argument('--api-user', required=False, type=str, default=None,
                               help='API user')
    create_parser.add_argument('--verify-ssl', required=False, type=bool, default=None,
                               help='Verify SSL')

    update_parser = sub_parser.add_parser('update', help='Update a credential')
    update_parser.add_argument('--name', required=False, type=str, default=None,
                               help='Name of the credential to update')

    update_parser.add_argument('--fqdn', required=False, type=str, default=None,
                               help='FQDN of the PCE')
    update_parser.add_argument('--port', required=False, type=int, default=None,
                               help='Port of the PCE')
    update_parser.add_argument('--org', required=False, type=int, default=None,
                               help='Organization ID')
    update_parser.add_argument('--api-user', required=False, type=str, default=None,
                               help='API user')
    update_parser.add_argument('--verify-ssl', required=False, type=bool, default=None,
                               help='Verify SSL')

    # Delete sub-command
    delete_parser = sub_parser.add_parser('delete', help='Delete a credential')
    delete_parser.add_argument('--name', required=False, type=str, default=None,
                               help='Name of the credential to delete')
    delete_parser.add_argument('--yes', '-y', action='store_true', default=False,
                               help='Skip confirmation prompt')

    # Web editor sub-command
    web_editor_parser = sub_parser.add_parser('web-editor', help='Start web-based credential editor')
    web_editor_parser.add_argument('--host', required=False, type=str, default='127.0.0.1',
                                   help='Host to bind the web server to')
    web_editor_parser.add_argument('--port', required=False, type=int, default=5000,
                                   help='Port to bind the web server to')


def __main(args, **kwargs):
    if args['sub_command'] == 'list':
        table = PrettyTable(field_names=["Name", "URL", "API User", "Originating File"])
        table.align = "l"

        credentials = get_all_credentials()
        credentials.sort(key=lambda x: x.name)

        for credential in credentials:
            table.add_row([credential.name, credential.fqdn, credential.api_user, credential.originating_file])

        print(table)

    elif args['sub_command'] == 'create':

        wanted_name = args['name']
        if wanted_name is None:
            wanted_name = click.prompt('> Input a Profile Name (ie: prod-pce)', type=str)

        print("* Checking if a credential with the same name already exists...", flush=True, end="")
        credentials = get_all_credentials()
        for credential in credentials:
            if credential.name == args['name']:
                raise pylo.PyloEx("A credential named '{}' already exists".format(args['name']))
        print("OK!")

        wanted_fqdn = args['fqdn']
        if wanted_fqdn is None:
            wanted_fqdn = click.prompt('> PCE FQDN (ie: pce1.mycompany.com)', type=str)

        wanted_port = args['port']
        if wanted_port is None:
            wanted_port = click.prompt('> PCE Port (ie: 8443)', type=int)

        wanted_org = args['org']
        if wanted_org is None:
            wanted_org = click.prompt('> Organization ID', type=int)

        wanted_api_user = args['api_user']
        if wanted_api_user is None:
            wanted_api_user = click.prompt('> API User', type=str)

        wanted_verify_ssl = args['verify_ssl']
        if wanted_verify_ssl is None:
            wanted_verify_ssl = click.prompt('> Verify SSL/TLS certificate? Y/N', type=bool)

        print()
        print("Recap:")
        print("Name: {}".format(wanted_name))
        print("FQDN: {}".format(wanted_fqdn))
        print("Port: {}".format(wanted_port))
        print("Org ID: {}".format(wanted_org))
        print("API User: {}".format(wanted_api_user))
        print("Verify SSL: {}".format(wanted_verify_ssl))
        print()

        # prompt of API key from user input, single line, hidden
        api_key = click.prompt('> API Key', hide_input=True)

        credentials_data: CredentialFileEntry = {
            "name": wanted_name,
            "fqdn": wanted_fqdn,
            "port": wanted_port,
            "org_id": wanted_org,
            "api_user": wanted_api_user,
            "verify_ssl": wanted_verify_ssl,
            "api_key": api_key
        }

        if is_encryption_available():
            encrypt_api_key = click.prompt('> Encrypt API (requires an SSH agent running and an RSA or Ed25519 key added to them) ? Y/N', type=bool)
            if encrypt_api_key:
                print("Available keys (ECDSA NISTPXXX keys and a few others are not supported and will be filtered out):")
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
                # encrypted_api_key = encrypt_api_key_with_paramiko_ssh_key_fernet(ssh_key=selected_ssh_key, api_key=api_key)
                encrypted_api_key = encrypt_api_key_with_paramiko_ssh_key_chacha20poly1305(ssh_key=selected_ssh_key, api_key=api_key)
                print("OK!")
                print(" * trying to decrypt the encrypted API key...", flush=True, end="")
                decrypted_api_key = decrypt_api_key_with_paramiko_ssh_key_chacha20poly1305(encrypted_api_key_payload=encrypted_api_key)
                if decrypted_api_key != api_key:
                    raise pylo.PyloEx("Decrypted API key does not match original API key")
                print("OK!")
                credentials_data["api_key"] = encrypted_api_key
        else:
            print(" * encryption is not available (no SSH agent or compatible key found), storing API key in plain text...")

        cwd = os.getcwd()
        create_in_current_workdir = click.prompt('> Create in current workdir ({})? If not then user homedir will be used.   Y/N '.format(cwd), type=bool)

        print("* Creating credential...", flush=True, end="")
        if create_in_current_workdir:
            file_path = create_credential_in_file(file_full_path=cwd, data=credentials_data)
        else:
            file_path = create_credential_in_default_file(data=credentials_data)

        print("OK! ({})".format(file_path))

    elif args['sub_command'] == 'update':
        # if name is not provided, prompt for it
        if args['name'] is None:
            wanted_name = click.prompt('> Input a Profile Name to update (ie: prod-pce)', type=str)
            args['name'] = wanted_name

        # find the credential by name
        wanted_name = args['name']
        found_profile = get_credentials_from_file(wanted_name, fail_with_an_exception=False)
        if found_profile is None:
            print("Cannot find a profile named '{}'".format(wanted_name))
            print("Available profiles:")
            credentials = get_all_credentials()
            for credential in credentials:
                print(" - {}".format(credential.name))
            sys.exit(1)

        print("Found profile '{}' to update in file '{}'".format(found_profile.name, found_profile.originating_file))

        if args['fqdn'] is not None:
            found_profile.fqdn = args['fqdn']
        if args['port'] is not None:
            found_profile.port = args['port']
        if args['org'] is not None:
            found_profile.org_id = args['org']
        if args['api_user'] is not None:
            found_profile.api_user = args['api_user']
        if args['verify_ssl'] is not None:
            found_profile.verify_ssl = args['verify_ssl']

        # ask if user wants to update API key
        update_api_key = click.prompt('> Do you want to update the API key? Y/N', type=bool)
        if update_api_key:
            api_key = click.prompt('> New API Key', hide_input=True)
            found_profile.api_key = api_key
        print()

        if is_encryption_available():
            encrypt_api_key = click.prompt('> Encrypt API (requires an SSH agent running and an RSA or Ed25519 key added to them) ? Y/N', type=bool)
            if encrypt_api_key:
                print("Available keys (ECDSA NISTPXXX keys and a few others are not supported and will be filtered out):")
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
                # encrypted_api_key = encrypt_api_key_with_paramiko_ssh_key_fernet(ssh_key=selected_ssh_key, api_key=found_profile.api_key)
                encrypted_api_key = encrypt_api_key_with_paramiko_ssh_key_chacha20poly1305(ssh_key=selected_ssh_key, api_key=found_profile.api_key)
                print("OK!")
                print(" * trying to decrypt the encrypted API key...", flush=True, end="")
                decrypted_api_key = decrypt_api_key_with_paramiko_ssh_key_chacha20poly1305(encrypted_api_key_payload=encrypted_api_key)
                if decrypted_api_key != found_profile.api_key:
                    raise pylo.PyloEx("Decrypted API key does not match original API key")
                print("OK!")
                found_profile.api_key = encrypted_api_key

        credentials_data: CredentialFileEntry = {
            "name": found_profile.name,
            "fqdn": found_profile.fqdn,
            "port": found_profile.port,
            "org_id": found_profile.org_id,
            "api_user": found_profile.api_user,
            "verify_ssl": found_profile.verify_ssl,
            "api_key": found_profile.api_key
        }

        print("* Updating credential in file '{}'...".format(found_profile.originating_file), flush=True, end="")
        create_credential_in_file(file_full_path=found_profile.originating_file, data=credentials_data, overwrite_existing_profile=True)
        print("OK!")

    elif args['sub_command'] == 'delete':
        # if name is not provided, prompt for it
        wanted_name = args['name']
        if wanted_name is None:
            wanted_name = click.prompt('> Input a Profile Name to delete (ie: prod-pce)', type=str)

        # find the credential by name
        found_profile = get_credentials_from_file(wanted_name, fail_with_an_exception=False)
        if found_profile is None:
            print("Cannot find a profile named '{}'".format(wanted_name))
            print("Available profiles:")
            credentials = get_all_credentials()
            for credential in credentials:
                print(" - {}".format(credential.name))
            sys.exit(1)

        print("Found profile '{}' in file '{}'".format(found_profile.name, found_profile.originating_file))
        print(" - FQDN: {}".format(found_profile.fqdn))
        print(" - Port: {}".format(found_profile.port))
        print(" - Org ID: {}".format(found_profile.org_id))
        print(" - API User: {}".format(found_profile.api_user))

        # Confirm deletion unless --yes flag is provided
        if not args['yes']:
            confirm = click.prompt('> Are you sure you want to delete this credential? Y/N', type=bool)
            if not confirm:
                print("Deletion cancelled.")
                sys.exit(0)

        print("* Deleting credential...", flush=True, end="")
        delete_credential_from_file(profile_name=found_profile.name, file_path=found_profile.originating_file)
        print("OK!")

    elif args['sub_command'] == 'test':
        print("* Profile Tester command")
        wanted_name = args['name']
        if wanted_name is None:
            wanted_name = click.prompt('> Input a Profile Name to test (ie: prod-pce)', type=str)
        found_profile = get_credentials_from_file(wanted_name, fail_with_an_exception=False)
        if found_profile is None:
            print("Cannot find a profile named '{}'".format(wanted_name))
            print("Available profiles:")
            credentials = get_all_credentials()
            for credential in credentials:
                print(" - {}".format(credential.name))
            sys.exit(1)

        print("Selected profile:")
        print(" - Name: {}".format(found_profile.name))
        print(" - FQDN: {}".format(found_profile.fqdn))
        print(" - Port: {}".format(found_profile.port))
        print(" - Org ID: {}".format(found_profile.org_id))
        print(" - API User: {}".format(found_profile.api_user))
        print(" - Verify SSL: {}".format(found_profile.verify_ssl))

        print("* Testing credential...", flush=True, end="")
        connector = pylo.APIConnector.create_from_credentials_object(found_profile)
        connector.objects_label_dimension_get()
        print("OK!")

    elif args['sub_command'] == 'web-editor':
        run_web_editor(host=args['host'], port=args['port'])

    else:
        raise pylo.PyloEx("Unknown sub-command '{}'".format(args['sub_command']))


command_object = Command(command_name, __main, fill_parser, credentials_manager_mode=True)


def print_keys(keys: list[paramiko.AgentKey], display_index=True) -> None:

    column_properties = [
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


def run_web_editor(host: str = '127.0.0.1', port: int = 5000) -> None:
    """Start the Flask web server for credential management."""
    try:
        from flask import Flask, jsonify, request, send_from_directory
    except ImportError:
        print("Flask is not installed. Please install it with: pip install flask")
        sys.exit(1)

    # Determine paths for static files
    current_dir = os.path.dirname(os.path.abspath(__file__))
    web_editor_dir = os.path.join(current_dir, 'ui/credential_manager_ui')
    # That directory should contain index.html, error if not
    if not os.path.exists(os.path.join(web_editor_dir, 'index.html')):
        print("Cannot find web editor static files in expected location: {}".format(web_editor_dir))
        sys.exit(1)

    app = Flask(__name__, static_folder=web_editor_dir)

    # Serve static files
    @app.route('/')
    def index():
        return send_from_directory(web_editor_dir, 'index.html')

    @app.route('/static/<path:filename>')
    def serve_static(filename):
        return send_from_directory(web_editor_dir, filename)

    # API: List all credentials
    @app.route('/api/credentials', methods=['GET'])
    def api_list_credentials():
        credentials = get_all_credentials()
        credentials.sort(key=lambda x: x.name)
        result = []
        for cred in credentials:
            result.append({
                'name': cred.name,
                'fqdn': cred.fqdn,
                'port': cred.port,
                'org_id': cred.org_id,
                'api_user': cred.api_user,
                'verify_ssl': cred.verify_ssl,
                'originating_file': cred.originating_file
            })
        return jsonify(result)

    # API: Get a single credential
    @app.route('/api/credentials/<name>', methods=['GET'])
    def api_get_credential(name):
        found_profile = get_credentials_from_file(name, fail_with_an_exception=False)
        if found_profile is None:
            return jsonify({'error': 'Credential not found'}), 404
        return jsonify({
            'name': found_profile.name,
            'fqdn': found_profile.fqdn,
            'port': found_profile.port,
            'org_id': found_profile.org_id,
            'api_user': found_profile.api_user,
            'verify_ssl': found_profile.verify_ssl,
            'originating_file': found_profile.originating_file
        })

    # API: Create a new credential
    @app.route('/api/credentials', methods=['POST'])
    def api_create_credential():
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        required_fields = ['name', 'fqdn', 'port', 'org_id', 'api_user', 'api_key']
        for field in required_fields:
            if field not in data:
                return jsonify({'error': f'Missing required field: {field}'}), 400

        # Check if credential already exists
        credentials = get_all_credentials()
        for credential in credentials:
            if credential.name == data['name']:
                return jsonify({'error': f"A credential named '{data['name']}' already exists"}), 400

        credentials_data: CredentialFileEntry = {
            "name": data['name'],
            "fqdn": data['fqdn'],
            "port": int(data['port']),
            "org_id": int(data['org_id']),
            "api_user": data['api_user'],
            "verify_ssl": data.get('verify_ssl', True),
            "api_key": data['api_key']
        }

        # Handle encryption if requested
        if data.get('encrypt') and data.get('ssh_key_index') is not None:
            if is_encryption_available():
                try:
                    ssh_keys = get_supported_keys_from_ssh_agent()
                    key_index = int(data['ssh_key_index'])
                    if 0 <= key_index < len(ssh_keys):
                        selected_ssh_key = ssh_keys[key_index]
                        encrypted_api_key = encrypt_api_key_with_paramiko_ssh_key_chacha20poly1305(
                            ssh_key=selected_ssh_key, api_key=data['api_key'])
                        # Verify encryption
                        decrypted_api_key = decrypt_api_key_with_paramiko_ssh_key_chacha20poly1305(
                            encrypted_api_key_payload=encrypted_api_key)
                        if decrypted_api_key != data['api_key']:
                            return jsonify({'error': 'Encryption verification failed'}), 500
                        credentials_data["api_key"] = encrypted_api_key
                except Exception as e:
                    return jsonify({'error': f'Encryption failed: {str(e)}'}), 500

        try:
            if data.get('use_current_workdir'):
                file_path = create_credential_in_file(file_full_path=os.getcwd(), data=credentials_data)
            else:
                file_path = create_credential_in_default_file(data=credentials_data)
            return jsonify({'success': True, 'file_path': file_path})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # API: Update a credential
    @app.route('/api/credentials/<name>', methods=['PUT'])
    def api_update_credential(name):
        data = request.get_json()
        if not data:
            return jsonify({'error': 'No data provided'}), 400

        found_profile = get_credentials_from_file(name, fail_with_an_exception=False)
        if found_profile is None:
            return jsonify({'error': 'Credential not found'}), 404

        # Update fields if provided
        if 'fqdn' in data:
            found_profile.fqdn = data['fqdn']
        if 'port' in data:
            found_profile.port = int(data['port'])
        if 'org_id' in data:
            found_profile.org_id = int(data['org_id'])
        if 'api_user' in data:
            found_profile.api_user = data['api_user']
        if 'verify_ssl' in data:
            found_profile.verify_ssl = data['verify_ssl']
        if 'api_key' in data and data['api_key']:
            found_profile.api_key = data['api_key']

        # Handle encryption if requested
        if data.get('encrypt') and data.get('ssh_key_index') is not None:
            if is_encryption_available():
                try:
                    ssh_keys = get_supported_keys_from_ssh_agent()
                    key_index = int(data['ssh_key_index'])
                    if 0 <= key_index < len(ssh_keys):
                        selected_ssh_key = ssh_keys[key_index]
                        encrypted_api_key = encrypt_api_key_with_paramiko_ssh_key_chacha20poly1305(
                            ssh_key=selected_ssh_key, api_key=found_profile.api_key)
                        decrypted_api_key = decrypt_api_key_with_paramiko_ssh_key_chacha20poly1305(
                            encrypted_api_key_payload=encrypted_api_key)
                        if decrypted_api_key != found_profile.api_key:
                            return jsonify({'error': 'Encryption verification failed'}), 500
                        found_profile.api_key = encrypted_api_key
                except Exception as e:
                    return jsonify({'error': f'Encryption failed: {str(e)}'}), 500

        credentials_data: CredentialFileEntry = {
            "name": found_profile.name,
            "fqdn": found_profile.fqdn,
            "port": found_profile.port,
            "org_id": found_profile.org_id,
            "api_user": found_profile.api_user,
            "verify_ssl": found_profile.verify_ssl,
            "api_key": found_profile.api_key
        }

        try:
            create_credential_in_file(file_full_path=found_profile.originating_file,
                                      data=credentials_data, overwrite_existing_profile=True)
            return jsonify({'success': True})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # API: Test a credential
    @app.route('/api/credentials/<name>/test', methods=['POST'])
    def api_test_credential(name):
        found_profile = get_credentials_from_file(name, fail_with_an_exception=False)
        if found_profile is None:
            return jsonify({'error': 'Credential not found'}), 404

        try:
            connector = pylo.APIConnector.create_from_credentials_object(found_profile)
            connector.objects_label_dimension_get()
            return jsonify({'success': True, 'message': 'Connection successful'})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # API: Delete a credential
    @app.route('/api/credentials/<name>', methods=['DELETE'])
    def api_delete_credential(name):
        found_profile = get_credentials_from_file(name, fail_with_an_exception=False)
        if found_profile is None:
            return jsonify({'error': 'Credential not found'}), 404

        try:
            delete_credential_from_file(profile_name=found_profile.name, file_path=found_profile.originating_file)
            return jsonify({'success': True, 'message': f"Credential '{name}' deleted successfully"})
        except Exception as e:
            return jsonify({'error': str(e)}), 500

    # API: Get SSH keys for encryption
    @app.route('/api/ssh-keys', methods=['GET'])
    def api_get_ssh_keys():
        if not is_encryption_available():
            return jsonify({'available': False, 'keys': []})

        try:
            ssh_keys = get_supported_keys_from_ssh_agent()
            keys_list = []
            for i, key in enumerate(ssh_keys):
                keys_list.append({
                    'index': i,
                    'type': key.get_name(),
                    'fingerprint': key.get_fingerprint().hex(),
                    'comment': key.comment
                })
            return jsonify({'available': True, 'keys': keys_list})
        except Exception as e:
            return jsonify({'available': False, 'error': str(e), 'keys': []})

    # API: Check encryption availability
    @app.route('/api/encryption-status', methods=['GET'])
    def api_encryption_status():
        return jsonify({'available': is_encryption_available()})

    print(f"Starting web editor at http://{host}:{port}")
    print("Press Ctrl+C to stop the server")
    app.run(host=host, port=port, debug=False)

