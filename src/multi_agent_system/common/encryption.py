"""Message encryption for secure communication."""

import base64
import hashlib
import logging
import os
from typing import Optional

logger = logging.getLogger('encryption')


class MessageEncryptor:
    """Handles message encryption and decryption."""

    def __init__(self, key: bytes = None):
        """
        Initialize with encryption key.
        If no key provided, generates a random key (not recommended for production).
        """
        self._key = key or self._generate_key()
        self._algorithm = 'AES-256-GCM'

    def _generate_key(self) -> bytes:
        """Generate a random 256-bit key."""
        return os.urandom(32)  # 256 bits

    def set_key(self, key: str):
        """Set key from hex string."""
        self._key = bytes.fromhex(key)

    def get_key_hex(self) -> str:
        """Get key as hex string for storage."""
        return self._key.hex()

    def encrypt(self, data: str) -> str:
        """
        Encrypt string data.
        Returns: base64 encoded encrypted data with IV prepended.
        """
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            aesgcm = AESGCM(self._key)

            # Generate random IV (nonce)
            iv = os.urandom(12)  # 96 bits for GCM

            # Encrypt
            ciphertext = aesgcm.encrypt(iv, data.encode('utf-8'), None)

            # Combine IV + ciphertext and base64 encode
            combined = iv + ciphertext
            return base64.b64encode(combined).decode('utf-8')
        except ImportError:
            logger.warning("cryptography library not available, using simple encoding")
            # Fallback: just base64 encode (NOT secure)
            return base64.b64encode(data.encode('utf-8')).decode('utf-8')

    def decrypt(self, encrypted_data: str) -> str:
        """
        Decrypt encrypted string data.
        Input: base64 encoded encrypted data with IV prepended.
        """
        try:
            from cryptography.hazmat.primitives.ciphers.aead import AESGCM
            aesgcm = AESGCM(self._key)

            # Decode from base64
            combined = base64.b64decode(encrypted_data.encode('utf-8'))

            # Extract IV and ciphertext
            iv = combined[:12]
            ciphertext = combined[12:]

            # Decrypt
            plaintext = aesgcm.decrypt(iv, ciphertext, None)
            return plaintext.decode('utf-8')
        except ImportError:
            logger.warning("cryptography library not available")
            # Fallback: just base64 decode
            return base64.b64decode(encrypted_data.encode('utf-8')).decode('utf-8')

    def encrypt_dict(self, data: dict) -> dict:
        """Encrypt all string values in a dictionary."""
        import json
        json_str = json.dumps(data)
        encrypted = self.encrypt(json_str)
        return {'_encrypted': encrypted, '_algorithm': self._algorithm}

    def decrypt_dict(self, encrypted_data: dict) -> dict:
        """Decrypt dictionary from encrypt_dict format."""
        import json
        if '_encrypted' in encrypted_data:
            json_str = self.decrypt(encrypted_data['_encrypted'])
            return json.loads(json_str)
        return encrypted_data


class PayloadEncryptor:
    """Encrypts message payloads for sensitive data."""

    def __init__(self, key: bytes = None):
        self._encryptor = MessageEncryptor(key)

    def encrypt_payload(self, payload: dict, sensitive_fields: list = None) -> dict:
        """
        Encrypt specified fields in payload.
        If sensitive_fields not specified, encrypt any field containing 'password', 'secret', 'token', 'key'.
        """
        import copy
        encrypted = copy.deepcopy(payload)
        fields_to_encrypt = sensitive_fields or []

        if not fields_to_encrypt:
            # Auto-detect sensitive fields
            for key in payload:
                if any(s in key.lower() for s in ['password', 'secret', 'token', 'key', 'credential']):
                    fields_to_encrypt.append(key)

        for field in fields_to_encrypt:
            if field in encrypted and isinstance(encrypted[field], str):
                encrypted[field] = self._encryptor.encrypt(encrypted[field])

        return encrypted

    def decrypt_payload(self, payload: dict, encrypted_fields: list = None) -> dict:
        """Decrypt specified fields in payload."""
        import copy
        decrypted = copy.deepcopy(payload)
        fields_to_decrypt = encrypted_fields or []

        # Auto-detect encrypted fields
        if not fields_to_decrypt and '_encrypted' in payload:
            # This is a fully encrypted payload
            return self._encryptor.decrypt_dict(payload)

        for field in fields_to_decrypt:
            if field in decrypted and isinstance(decrypted[field], str):
                try:
                    decrypted[field] = self._encryptor.decrypt(decrypted[field])
                except:
                    pass  # Not encrypted

        return decrypted


def generate_api_key(prefix: str = "sk") -> str:
    """Generate a random API key."""
    key_bytes = os.urandom(32)
    return f"{prefix}_{base64.urlsafe_b64encode(key_bytes).decode('utf-8')[:44]}"


def hash_sensitive(value: str) -> str:
    """Create a hash of sensitive value for comparison without storing plain text."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


# Global encryptor instance (should be initialized with real key in production)
_encryptor = MessageEncryptor()


def get_encryptor() -> MessageEncryptor:
    return _encryptor