CREATE TABLE IF NOT EXISTS sync (
  task VARCHAR(100) NOT NULL UNIQUE,
  synced_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

