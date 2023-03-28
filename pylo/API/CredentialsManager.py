import json
import os
from typing import Dict, TypedDict, Union, List

from ..Exception import PyloEx
from .. import log


class CredentialFileEntry(TypedDict):
    name: str
    hostname: str
    port: int
    api_user: str
    api_key: str
    org_id: int
    verify_ssl: bool


CredentialsFileType = Union[CredentialFileEntry | List[CredentialFileEntry]]


def check_profile_json_structure(profile: Dict) -> None:
    # ensure all fields from CredentialFileEntry are present
    if "name" not in profile or type(profile["name"]) != str:
        raise PyloEx("The profile {} does not contain a name".format(profile))
    if "hostname" not in profile:
        raise PyloEx("The profile {} does not contain a hostname".format(profile))
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


def get_credentials_from_file(hostname_or_profile_name: str = None,
                              credential_file: str = None) -> CredentialFileEntry:
    """
    Credentials files will be looked for in the following order:
    1. The path provided in the credential_file argument
    2. The path provided in the PYLO_CREDENTIAL_FILE environment variable
    3. The path ~/.pylo/credentials.json
    4. Current working directory credentials.json
    """
    if hostname_or_profile_name is None:
        log.debug("No hostname_or_profile_name provided, profile_name=default will be used")
        hostname_or_profile_name = "default"

    if credential_file is None:
        log.debug("No credential_file provided, looking for one in the environment variable PYLO_CREDENTIAL_FILE")
        credential_file = os.environ.get('PYLO_CREDENTIAL_FILE', None)
        if credential_file is None:
            log.debug("No credential_file provided, looking for one in the default path ~/.pylo/credentials.json")
            credential_file = os.path.expanduser("~/.pylo/credentials.json")
            if not os.path.exists(credential_file):
                log.debug("No credential_file provided, looking for one in the current working directory credentials.json")
                credential_file = os.path.join(os.getcwd(), "credentials.json")
                if not os.path.exists(credential_file):
                    raise PyloEx("No credential file found. Please provide a path to a credential file or create one "
                                 "in the default location ~/.pylo/credentials.json")

        log.debug("Loading credentials from file: {}".format(credential_file))
        with open(credential_file, 'r') as f:
            credentials: CredentialsFileType = json.load(f)
            found_profile = None
            available_profiles_names: List[str] = []
            # if it is a list, we need to find the right one
            if isinstance(credentials, list):
                for profile in credentials:
                    check_profile_json_structure(profile)
                    available_profiles_names.append(profile['name'])
                    if profile['name'].lower() == hostname_or_profile_name.lower():
                        found_profile = profile
                        break
                    if profile['hostname'].lower() == hostname_or_profile_name.lower():
                        found_profile = profile
                        break
                if found_profile is None:
                    raise PyloEx("No profile found in credential file '{}' with hostname/profile: {}."
                                 " Available profiles are: {}".
                                 format(credential_file, hostname_or_profile_name, available_profiles_names))

            else:
                log.debug("Credentials file is not a list, assuming it is a single profile")
                check_profile_json_structure(credentials)
                found_profile = credentials

            return found_profile

        raise PyloEx("No profile found in credential file '{}' with hostname: {}".
                    format(credential_file, hostname_or_profile_name))