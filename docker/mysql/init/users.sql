CREATE TABLE IF NOT EXISTS users (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  status VARCHAR(20) NOT NULL,
  country_id INT UNSIGNED NOT NULL,
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  FOREIGN KEY (country_id) REFERENCES countries(id) ON DELETE CASCADE
);

INSERT INTO users (name, status, country_id) VALUES 
  ('John Doe', 'active', 1),
  ('Jane Smith', 'inactive', 2),
  ('Alice Johnson', 'active', 3),
  ('Bob Brown', 'inactive', 4),
  ('Charlie Davis', 'active', 5),
  ('Diana Evans', 'inactive', 6),
  ('Ethan Foster', 'active', 7),
  ('Fiona Green', 'inactive', 8),
  ('George Harris', 'active', 9),
  ('Hannah Iversen', 'inactive', 10)
;
