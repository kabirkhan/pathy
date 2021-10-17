import os
from typing import Optional, Tuple, Union


def azure_credentials_from_env() -> Optional[Union[str, Tuple[str, str]]]:
    """Extract the Account URL and Credential (Access Key, or SAS token) or a full
    Azure Storage Connection String from the environment"""
    credentials = None
    conn_str: Optional[str] = os.getenv("PATHY_AZURE_CONN_STR")
    if conn_str is not None:
        credentials = conn_str
    else:
        account_url: Optional[str] = os.getenv("PATHY_AZURE_ACCOUNT_URL")
        credential: Optional[str] = os.getenv("PATHY_AZURE_CREDENTIAL")
        credentials = (account_url, credential)
    return credentials
