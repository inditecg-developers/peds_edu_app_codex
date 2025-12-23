from __future__ import annotations

import base64
from functools import lru_cache
from typing import Optional

try:
    import boto3
    from botocore.exceptions import ClientError
except Exception:  # pragma: no cover
    boto3 = None  # type: ignore
    ClientError = Exception  # type: ignore


@lru_cache(maxsize=32)
def get_secret_string(secret_name: str, region_name: str = "ap-south-1") -> Optional[str]:
    """
    Fetch a secret string from AWS Secrets Manager.

    This follows the approach you provided (boto3 session + secretsmanager client).
    Returns None if the secret can't be fetched in the current environment.
    """
    if boto3 is None:
        return None

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError:
        return None

    if "SecretString" in response and response["SecretString"]:
        return str(response["SecretString"]).strip()

    if "SecretBinary" in response and response["SecretBinary"]:
        try:
            return base64.b64decode(response["SecretBinary"]).decode("utf-8").strip()
        except Exception:
            return None

    return None
