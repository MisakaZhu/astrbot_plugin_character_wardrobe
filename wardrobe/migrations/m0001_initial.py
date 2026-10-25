"""迁移 v1：初始表结构。

外键全部开启并强制角色归属（复合外键），任何表都无法引用其他角色的对象。
"""

from __future__ import annotations

from . import register as mig_register

SQL = """
CREATE TABLE schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE roles (
    role_id TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL
);

CREATE TABLE persona_bindings (
    host_persona_key TEXT PRIMARY KEY,
    role_id TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    binding_revision INTEGER NOT NULL DEFAULT 1,
    bound_at TEXT NOT NULL,
    bound_by_ref TEXT NOT NULL
);
CREATE INDEX idx_bindings_role ON persona_bindings(role_id);

CREATE TABLE session_grants (
    role_id TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    umo_ref TEXT NOT NULL,
    permission_level TEXT NOT NULL
        CHECK (permission_level IN ('reader', 'contributor', 'maintainer')),
    allow_ai_mutation INTEGER NOT NULL DEFAULT 0 CHECK (allow_ai_mutation IN (0, 1)),
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (role_id, umo_ref)
);

CREATE TABLE wardrobe_items (
    item_id TEXT PRIMARY KEY,
    role_id TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    category TEXT NOT NULL,
    tags_json TEXT NOT NULL DEFAULT '[]',
    layerable INTEGER NOT NULL DEFAULT 0 CHECK (layerable IN (0, 1)),
    source_kind TEXT NOT NULL CHECK (source_kind IN ('user', 'ai')),
    created_by_ref TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
    revision INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_items_role_item ON wardrobe_items(role_id, item_id);
CREATE INDEX idx_items_role_active ON wardrobe_items(role_id, active, category);

CREATE TABLE image_assets (
    asset_id TEXT PRIMARY KEY,
    role_id TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    processed_sha256 TEXT NOT NULL,
    storage_key TEXT NOT NULL UNIQUE,
    mime TEXT NOT NULL CHECK (mime IN ('image/jpeg', 'image/png', 'image/webp')),
    byte_size INTEGER NOT NULL,
    width INTEGER NOT NULL,
    height INTEGER NOT NULL,
    auto_caption TEXT,
    caption_state TEXT NOT NULL DEFAULT 'none' CHECK (caption_state IN ('none', 'pending')),
    status TEXT NOT NULL DEFAULT 'ready' CHECK (status IN ('ready', 'orphaned')),
    created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_assets_role_asset ON image_assets(role_id, asset_id);
CREATE UNIQUE INDEX idx_assets_role_hash ON image_assets(role_id, processed_sha256);

CREATE TABLE item_images (
    role_id TEXT NOT NULL,
    item_id TEXT NOT NULL REFERENCES wardrobe_items(item_id) ON DELETE CASCADE,
    asset_id TEXT NOT NULL REFERENCES image_assets(asset_id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (item_id, asset_id),
    FOREIGN KEY (role_id, item_id) REFERENCES wardrobe_items(role_id, item_id) ON DELETE CASCADE,
    FOREIGN KEY (role_id, asset_id) REFERENCES image_assets(role_id, asset_id) ON DELETE CASCADE
);
CREATE INDEX idx_item_images_asset ON item_images(asset_id);

CREATE TABLE style_profiles (
    role_id TEXT PRIMARY KEY REFERENCES roles(role_id) ON DELETE CASCADE,
    preferred_tags_json TEXT NOT NULL DEFAULT '[]',
    avoid_tags_json TEXT NOT NULL DEFAULT '[]',
    scene_preferences TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE outfit_state (
    role_id TEXT PRIMARY KEY REFERENCES roles(role_id) ON DELETE CASCADE,
    revision INTEGER NOT NULL DEFAULT 0,
    source_kind TEXT NOT NULL DEFAULT 'user' CHECK (source_kind IN ('user', 'ai')),
    manual_lock_until TEXT,
    manual_lock_by_ref TEXT,
    updated_at TEXT NOT NULL
);

CREATE TABLE outfit_items (
    role_id TEXT NOT NULL,
    item_id TEXT NOT NULL REFERENCES wardrobe_items(item_id) ON DELETE CASCADE,
    slot TEXT NOT NULL,
    outfit_revision INTEGER NOT NULL,
    PRIMARY KEY (role_id, item_id, slot)
);

CREATE TABLE autonomy_state (
    role_id TEXT PRIMARY KEY REFERENCES roles(role_id) ON DELETE CASCADE,
    add_enabled INTEGER NOT NULL DEFAULT 1 CHECK (add_enabled IN (0, 1)),
    wear_enabled INTEGER NOT NULL DEFAULT 1 CHECK (wear_enabled IN (0, 1)),
    master_enabled INTEGER NOT NULL DEFAULT 1 CHECK (master_enabled IN (0, 1)),
    revision INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE operation_ledger (
    role_id TEXT NOT NULL REFERENCES roles(role_id) ON DELETE CASCADE,
    request_key TEXT NOT NULL,
    action TEXT NOT NULL,
    result_ref TEXT,
    status TEXT NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (role_id, request_key)
);

CREATE TABLE audit_events (
    audit_id INTEGER PRIMARY KEY AUTOINCREMENT,
    role_id TEXT,
    action TEXT NOT NULL,
    actor_ref TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    result TEXT NOT NULL,
    created_at TEXT NOT NULL
);
"""

mig_register(1, SQL.strip())
