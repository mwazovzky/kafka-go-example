CREATE TABLE IF NOT EXISTS countries (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  code VARCHAR(255) NOT NULL UNIQUE,
  name VARCHAR(255) NOT NULL
);

INSERT INTO countries (code, name) VALUES 
  ('USA', 'United States'),
  ('GBR', 'United Kingdom'),
  ('DEU', 'Germany'),
  ('FRA', 'France'),
  ('ESP', 'Spain'),
  ('ITA', 'Italy'),
  ('AUSTR', 'Australia'),
  ('JPN', 'Japan'),
  ('CHN', 'China'),
  ('IND', 'India'),
  ('BRA', 'Brazil'),
  ('RUS', 'Russia'),
  ('ZAF', 'South Africa'),
  ('ARG', 'Argentina'),
  ('SGP', 'Singapore'),
  ('HKG', 'Hong Kong'),
  ('KOR', 'South Korea'),
  ('TWN', 'Taiwan'),
  ('GEO', 'Georgia')
;

