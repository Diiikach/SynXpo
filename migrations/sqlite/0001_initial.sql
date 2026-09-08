CREATE TABLE directories (
    id TEXT PRIMARY KEY NOT NULL,
    current_revision INTEGER NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
    created_at_ms INTEGER NOT NULL
);

CREATE TABLE upload_sessions (
    id TEXT PRIMARY KEY NOT NULL,
    directory_id TEXT NOT NULL REFERENCES directories(id),
    owner_id TEXT NOT NULL,
    base_revision INTEGER NOT NULL CHECK (base_revision >= 0),
    state TEXT NOT NULL CHECK (state IN ('created', 'uploading', 'validating', 'committing', 'committed', 'aborted', 'expired')),
    manifest_hash TEXT,
    idempotency_key TEXT,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL,
    expires_at_ms INTEGER NOT NULL,
    committed_revision INTEGER CHECK (committed_revision IS NULL OR committed_revision >= 0),
    error_code TEXT,
    error_message TEXT,
    UNIQUE(directory_id, owner_id, idempotency_key)
);

CREATE INDEX upload_sessions_by_directory_state ON upload_sessions(directory_id, state);
CREATE INDEX upload_sessions_by_expiry ON upload_sessions(expires_at_ms);

CREATE TABLE upload_session_files (
    session_id TEXT NOT NULL REFERENCES upload_sessions(id) ON DELETE CASCADE,
    file_id TEXT,
    path TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (operation IN ('create', 'update', 'delete', 'rename')),
    expected_version INTEGER CHECK (expected_version IS NULL OR expected_version >= 0),
    content_hash TEXT,
    size INTEGER CHECK (size IS NULL OR size >= 0),
    staging_path TEXT,
    received_bytes INTEGER NOT NULL DEFAULT 0 CHECK (received_bytes >= 0),
    state TEXT NOT NULL CHECK (state IN ('pending', 'uploading', 'complete', 'failed')),
    PRIMARY KEY(session_id, path)
);

CREATE INDEX upload_session_files_by_session_state ON upload_session_files(session_id, state);
