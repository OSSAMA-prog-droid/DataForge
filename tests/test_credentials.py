import pytest
import json


# DF-13: Source credentials stored as plaintext
class TestCredentials:
    def test_credentials_stored_as_plaintext_json(self):
        # The SourceCredential model stores credentials as a JSON column with no encryption.
        # Any SELECT on the table returns passwords in plaintext.
        from src.models.pipeline import SourceCredential

        cred = SourceCredential(
            id="cred-1",
            pipeline_id="pipe-1",
            credential_type="postgres",
            credentials={
                "host": "prod-db.internal",
                "port": 5432,
                "user": "dataforge_reader",
                "password": "SuperSecret123!",   # plaintext
                "database": "client_production",
            },
        )

        # Bug: credentials are a plain dict — readable by anyone with DB access
        assert isinstance(cred.credentials, dict)
        assert cred.credentials["password"] == "SuperSecret123!"

        # Fix: credentials should be an encrypted bytes column
        # e.g. Fernet(key).encrypt(json.dumps(credentials).encode())
        # and decrypted only at pipeline runtime

    def test_api_key_stored_as_plaintext(self):
        from src.models.pipeline import SourceCredential

        cred = SourceCredential(
            id="cred-2",
            pipeline_id="pipe-2",
            credential_type="rest_api",
            credentials={
                "base_url": "https://api.client.com",
                "api_key": "sk-live-abc123def456",  # plaintext API key
            },
        )
        # Any SQL dump, backup file, or DB read exposes this key
        assert cred.credentials["api_key"] == "sk-live-abc123def456"
