CREATE TABLE schema_migrations (
    version BIGINT PRIMARY KEY,
    name TEXT NOT NULL
);

CREATE TABLE directories (
    id UUID PRIMARY KEY,
    current_revision BIGINT NOT NULL DEFAULT 0 CHECK (current_revision >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE upload_sessions (
    id UUID PRIMARY KEY,
    directory_id UUID NOT NULL REFERENCES directories(id),
    owner_id TEXT NOT NULL,
    base_revision BIGINT NOT NULL CHECK (base_revision >= 0),
    state TEXT NOT NULL,
    manifest_hash TEXT,
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    committed_revision BIGINT,
    error_code TEXT,
    error_message TEXT,
    UNIQUE (directory_id, owner_id, idempotency_key)
);

CREATE TABLE upload_session_files (
    session_id UUID NOT NULL REFERENCES upload_sessions(id) ON DELETE CASCADE,
    file_id UUID,
    path TEXT NOT NULL,
    operation TEXT NOT NULL,
    expected_version BIGINT,
    content_hash TEXT,
    size BIGINT,
    staging_path TEXT,
    received_bytes BIGINT NOT NULL DEFAULT 0,
    state TEXT NOT NULL,
    PRIMARY KEY (session_id, path)
);
