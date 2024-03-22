import base64
from hashlib import sha256
from typing import Dict, TypedDict, Union, List, Optional
import json
import os
from cryptography.fernet import Fernet
from ..Exception import PyloEx
from .. import log

try:
    import paramiko
except ImportError:
    log.debug("Paramiko library not found, SSH based encryption will not be available")
    paramiko = None



class CredentialFileEntry(TypedDict):
    name: str
    fqdn: str
    port: int
    api_user: str
    api_key: str
    org_id: int
    verify_ssl: bool


class CredentialProfile:
    name: str
    fqdn: str
    port: int
    api_user: str
    api_key: str
    org_id: int
    verify_ssl: bool

    def __init__(self, name: str, fqdn: str, port: int, api_user: str, api_key: str, org_id: int, verify_ssl: bool, originating_file: Optional[str] = None):
        self.name = name
        self.fqdn = fqdn
        self.port = port
        self.api_user = api_user
        self.api_key = api_key
        self.org_id = org_id
        self.verify_ssl = verify_ssl
        self.originating_file = originating_file

        self.raw_json: Optional[CredentialFileEntry] = None


    @staticmethod
    def from_credentials_file_entry(credential_file_entry: CredentialFileEntry, originating_file: Optional[str] = None):
        return CredentialProfile(credential_file_entry['name'],
                                 credential_file_entry['fqdn'],
                                 credential_file_entry['port'],
                                 credential_file_entry['api_user'],
                                 credential_file_entry['api_key'],
                                 credential_file_entry['org_id'],
                                 credential_file_entry['verify_ssl'],
                                 originating_file)


CredentialsFileType = Union[CredentialFileEntry | List[CredentialFileEntry]]


def check_profile_json_structure(profile: Dict) -> None:
    # ensure all fields from CredentialFileEntry are present
    if "name" not in profile or type(profile["name"]) != str:
        raise PyloEx("The profile {} does not contain a name".format(profile))
    if "fqdn" not in profile:
        raise PyloEx("The profile {} does not contain a fqdn".format(profile))
    if "port" not in profile:
        raise PyloEx("The profile {} does not contain a port".format(profile))
    if "api_user" not in profile:
        raise PyloEx("The profile {} does not contain an api_user".format(profile))
    if "api_key" not in profile:
        raise PyloEx("The profile {} does not contain an api_key".format(profile))
    if "org_id" not in profile:
        raise PyloEx("The profile {} does not contain an organization_id".format(profile))
    if "verify_ssl" not in profile:
        raise PyloEx("The profile {} does not contain a verify_ssl".format(profile))


def get_all_credentials_from_file(credential_file: str ) -> List[CredentialProfile]:
    log.debug("Loading credentials from file: {}".format(credential_file))
    with open(credential_file, 'r') as f:
        credentials: CredentialsFileType = json.load(f)
        profiles: List[CredentialProfile] = []
        if isinstance(credentials, list):
            for profile in credentials:
                check_profile_json_structure(profile)
                profiles.append(CredentialProfile.from_credentials_file_entry(profile, credential_file))
        else:
            check_profile_json_structure(credentials)
            profiles.append(CredentialProfile.from_credentials_file_entry(credentials, credential_file))

        return profiles


def get_credentials_from_file(fqdn_or_profile_name: str = None,
                              credential_file: str = None, fail_with_an_exception=True) -> Optional[CredentialProfile]:

    if fqdn_or_profile_name is None:
        log.debug("No fqdn_or_profile_name provided, profile_name=default will be used")
        fqdn_or_profile_name = "default"

    credential_files: List[str] = []
    if credential_file is not None:
        credential_files.append(credential_file)
    else:
        credential_files = list_potential_credential_files()

    credentials: List[CredentialProfile] = []

    for file in credential_files:
        log.debug("Loading credentials from file: {}".format(credential_file))
        credentials.extend(get_all_credentials_from_file(file))

    for credential_profile in credentials:
        if credential_profile.name.lower() == fqdn_or_profile_name.lower():
            return credential_profile
        if credential_profile.fqdn.lower() == fqdn_or_profile_name.lower():
            return credential_profile

    if fail_with_an_exception:
        raise PyloEx("No profile found in credential file '{}' with fqdn: {}".
                    format(credential_file, fqdn_or_profile_name))

    return None


def list_potential_credential_files() -> List[str]:
    """
    List the potential locations where a credential file could be found and return them if they exist
    :return:
    """
    potential_credential_files = []
    if os.environ.get('Pylo_CREDENTIAL_FILE', None) is not None:
        potential_credential_files.append(os.environ.get('Pylo_CREDENTIAL_FILE'))
    potential_credential_files.append(os.path.expanduser("~/.pylo/credentials.json"))
    potential_credential_files.append(os.path.join(os.getcwd(), "credentials.json"))

    return [file for file in potential_credential_files if os.path.exists(file)]


def get_all_credentials() -> List[CredentialProfile]:
    """
    Get all credentials from all potential credential files
    :return:
    """
    credential_files = list_potential_credential_files()
    credentials = []
    for file in credential_files:
        credentials.extend(get_all_credentials_from_file(file))
    return credentials


def create_credential_in_file(file_full_path: str, data: CredentialFileEntry, overwrite_existing_profile = False) -> str:
    """
    Create a credential in a file and return the full path to the file
    :param file_full_path:
    :param data:
    :param overwrite_existing_profile:
    :return:
    """
    # if file already exists, load it and append the new credential to it
    if os.path.isdir(file_full_path):
        file_full_path = os.path.join(file_full_path, "credentials.json")

    if os.path.exists(file_full_path):
        with open(file_full_path, 'r') as f:
            credentials: CredentialsFileType = json.load(f)
            if isinstance(credentials, list):
                # check if the profile already exists
                for profile in credentials:
                    if profile['name'].lower() == data['name'].lower():
                        if overwrite_existing_profile:
                            profile = data
                            break
                        else:
                            raise PyloEx("Profile with name {} already exists in file {}".format(data['name'], file_full_path))
                credentials.append(data)
            else:
                if data['name'].lower() == credentials['name'].lower():
                    if overwrite_existing_profile:
                        credentials = data
                    else:
                        raise PyloEx("Profile with name {} already exists in file {}".format(data['name'], file_full_path))
                else:
                    credentials = [credentials, data]
    else:
            credentials = [data]

    # write to the file
    with open(file_full_path, 'w') as f:
        json.dump(credentials, f, indent=4)

    return file_full_path

def create_credential_in_default_file(data: CredentialFileEntry) -> str:
    """
    Create a credential in the default credential file and return the full path to the file
    :param data:
    :return:
    """
    file_path = os.path.expanduser("~/.pylo/credentials.json")
    create_credential_in_file(os.path.expanduser(file_path), data)
    return file_path


def encrypt_api_key_with_paramiko_ssh_key_fernet(ssh_key: paramiko.AgentKey, api_key: str) -> str:
    def encrypt(raw: str, key: bytes) -> bytes:
        """

        :param raw:
        :param key:
        :return: base64 encoded encrypted string
        """
        f = Fernet(base64.urlsafe_b64encode(key))
        token = f.encrypt(bytes(raw, 'utf-8'))
        return token


    # generate a random 128bit key
    session_key_to_sign = os.urandom(32)

    signed_message = ssh_key.sign_ssh_data(session_key_to_sign)

    # use SHA256 to hash the signed message and use it as final AES 256 key
    encryption_key = sha256(signed_message).digest()
    #print("Encryption key: {}".format(encryption_key.hex()))
    encrypted_text = encrypt(api_key, encryption_key)

    api_key = "$encrypted$:ssh-Fernet:{}:{}:{}".format(base64.urlsafe_b64encode(ssh_key.get_fingerprint()).decode('utf-8'),
                                                       base64.urlsafe_b64encode(session_key_to_sign).decode('utf-8'),
                                                       encrypted_text.decode('utf-8'))

    return api_key


def decrypt_api_key_with_paramiko_ssh_key_fernet(encrypted_api_key_payload: str) -> str:
    def decrypt(token_b64_encoded: str, key: bytes):
        f = Fernet(base64.urlsafe_b64encode(key))
        return f.decrypt(token_b64_encoded).decode('utf-8')

    # split the api_key into its components
    api_key_parts = encrypted_api_key_payload.split(":")
    if len(api_key_parts) != 5:
        raise PyloEx("Invalid encrypted API key format")

    # get the fingerprint and the session key
    fingerprint = base64.urlsafe_b64decode(api_key_parts[2])
    session_key = base64.urlsafe_b64decode(api_key_parts[3])
    encrypted_api_key = api_key_parts[4]

    # find the key in the agent
    keys = paramiko.Agent().get_keys()
    found_key = None
    for key in keys:
        if key.get_fingerprint() == fingerprint:
            found_key = key
            break

    if found_key is None:
        raise PyloEx("No key found in the agent with fingerprint {}".format(fingerprint.hex()))

    # sign the session key
    signed_session_key = found_key.sign_ssh_data(session_key)
    encryption_key = sha256(signed_session_key).digest()
    #print("Encryption key: {}".format(encryption_key.hex()))
    #print("Encrypted from KEY fingerprint: {}".format(fingerprint.hex()))

    return decrypt(token_b64_encoded=encrypted_api_key,
                   key=encryption_key
                   )

def decrypt_api_key(encrypted_api_key_payload: str) -> str:
    # detect the encryption method
    if not encrypted_api_key_payload.startswith("$encrypted$:"):
        raise PyloEx("Invalid encrypted API key format")
    if encrypted_api_key_payload.startswith("$encrypted$:ssh-Fernet:"):
        return decrypt_api_key_with_paramiko_ssh_key_fernet(encrypted_api_key_payload)

    raise PyloEx("Unsupported encryption method: {}".format(encrypted_api_key_payload.split(":")[1]))


def is_api_key_encrypted(encrypted_api_key_payload: str) -> bool:
    return encrypted_api_key_payload.startswith("$encrypted$:")