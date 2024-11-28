import hashlib
from pathlib import Path
from typing import Tuple, Union
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


def load_rsa_public_key(key: Union[Path | str]) -> rsa.RSAPublicKey:
    """
    Load an RSA public key from a file in PEM format.

    Args:
        key_path: Path of public key  or string to the public key file content

    Returns:
        RSAPublicKey: Loaded public key object

    Raises:
        ValueError: If file doesn't contain a valid RSA public key
        FileNotFoundError: If key file doesn't exist
    """
    key_data = None
    if isinstance(key, Path):
        with open(key, "rb") as f:
            key_data = f.read()
    else:
        key_data = bytes(key)

    try:
        public_key = serialization.load_pem_public_key(key_data)
        if not isinstance(public_key, rsa.RSAPublicKey):
            raise ValueError("Not an RSA public key")
        return public_key
    except Exception as e:
        raise ValueError(f"Failed to load public key: {e}")


def get_key_fingerprint(public_key: rsa.RSAPublicKey) -> str:
    """
    Calculate SHA256 fingerprint of an RSA public key.

    Args:
        public_key: RSA public key object

    Returns:
        str: SHA256 fingerprint as hex string
    """
    # Get DER encoding of the public key
    public_bytes = public_key.public_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    # Calculate SHA256 hash
    fingerprint = hashlib.sha256(public_bytes).hexdigest()
    return fingerprint


def match_fingerprints(fp_1: str, fp_2: str) -> bool:
    """
    Compare two RSA public keys by their fingerprints.

    Args:
        fp_1: First Public Key FingerPrint
        fp_2: Second Public Key FingerPrint

    Returns:
        Tuple containing:
            - Boolean indicating if keys match
            - Fingerprint of first key
            - Fingerprint of second key
    """
    # Compare
    return fp_1 == fp_2
