import json
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from src.models.pipeline import SourceCredential

# BUG DF-13: Source connector credentials (DB passwords, API keys, S3 secrets)
# are stored as plaintext JSON in the source_credentials table.
# Any developer with DB read access, any backup file, any SQL injection
# vulnerability, or any accidental log of the credentials column exposes
# all connected data source passwords simultaneously.
# Fix: encrypt the credentials column using Fernet symmetric encryption
# (key stored in env var / secrets manager), decrypt only at pipeline runtime.


async def save_credentials(
    session: AsyncSession,
    pipeline_id: str,
    credential_type: str,
    credentials: dict,
) -> SourceCredential:
    import uuid
    # BUG DF-13: credentials dict written as-is — passwords in plaintext
    cred = SourceCredential(
        id=str(uuid.uuid4()),
        pipeline_id=pipeline_id,
        credential_type=credential_type,
        credentials=credentials,  # { "host": "...", "password": "supersecret", "api_key": "..." }
    )
    session.add(cred)
    await session.commit()
    return cred


async def load_credentials(
    session: AsyncSession,
    pipeline_id: str,
) -> dict:
    result = await session.execute(
        select(SourceCredential).where(SourceCredential.pipeline_id == pipeline_id)
    )
    cred = result.scalar_one_or_none()
    if not cred:
        raise ValueError(f"No credentials found for pipeline {pipeline_id}")
    # BUG DF-13: returns plaintext dict — no decryption step because nothing was encrypted
    return cred.credentials
