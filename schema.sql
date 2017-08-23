CREATE TABLE IF NOT EXISTS billing_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    object_type TEXT CHECK( object_type IN ('env','farm','farm_role', 'server') ) NOT NULL,
    object_id TEXT NOT NULL,
    cost REAL NOT NULL
);
