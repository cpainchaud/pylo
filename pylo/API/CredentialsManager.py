from typing import Dict, TypedDict, Union, List, Optional
import json
import os
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
                              credential_file: str = None) -> CredentialProfile:

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

    raise PyloEx("No profile found in credential file '{}' with fqdn: {}".
                    format(credential_file, fqdn_or_profile_name))


def list_potential_credential_files() -> List[str]:
    """
    List the potential locations where a credential file could be found and return them if they exist
    :return:
    """
    potential_credential_files = []
    if os.environ.get('PYLO_CREDENTIAL_FILE', None) is not None:
        potential_credential_files.append(os.environ.get('PYLO_CREDENTIAL_FILE'))
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


def create_credential_in_file(file_full_path: str, data: CredentialFileEntry, overwrite_existing_profile = False) -> None:
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

def create_credential_in_default_file(data: CredentialFileEntry) -> None:
    create_credential_in_file(os.path.expanduser("~/.pylo/credentials.json"), data)
