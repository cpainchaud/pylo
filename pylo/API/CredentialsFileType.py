from typing import TypedDict, Union, List


class CredentialEntry(TypedDict):
    name: str
    hostname: str
    port: int
    api_user: str
    api_key: str
    org_id: int
    verify_ssl: bool


CredentialsFileType = Union[CredentialEntry|List[CredentialEntry]]
