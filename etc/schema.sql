CREATE TABLE doi_prefixes (
  id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
  prefix VARCHAR(16)
) ENGINE=InnoDB CHARACTER SET=utf8mb4;

CREATE UNIQUE INDEX doi_prefixes_prefix ON doi_prefixes(prefix);

CREATE TABLE resource_url_domains (
  id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
  domain VARCHAR(1024),
  h_proportion_resource_equals_naive_destination_url FLOAT NULL DEFAULT NULL
  h_proportion_resource_equals_browser_destination_url FLOAT NULL DEFAULT NULL

) ENGINE=InnoDB CHARACTER SET=utf8mb4;

CREATE UNIQUE INDEX resource_url_domains_domain ON resource_url_domains(domain(128));


CREATE TABLE items (
  id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
  prefix_id INTEGER REFERENCES doi_prefixes(id),
  resource_url_domain_id INTEGER REFERENCES resource_url_domains(id),
  doi VARCHAR(2048) NOT NULL,
  resource_url VARCHAR(2048) NULL,
  resource_url_updated DATETIME,
  error_code INTEGER NULL,
  checked_resource_url_unique BOOLEAN NOT NULL DEFAULT FALSE,
  --resource_url_unique BOOLEAN NOT NULL DEFAULT FALSE,
  meta_type INTEGER NOT NULL DEFAULT 0,
  defer_to_item_id INTEGER REFERENCES items(id),

  naive_destination_url VARCHAR(2048) NULL,
  naive_destination_url_updated DATETIME NULL,

  -- heuristics
  h_duplicate_naive_destination_url INTEGER NULL,
  h_duplicate_resource_url INTEGER NULL,
  h_deleted BOOLEAN NOT NULL DEFAULT FALSE,
  h_resource_equals_browser_destination_url BOOLEAN NULL DEFAULT NULL,
  h_naive_equals_browser_destination_url  BOOLEAN NULL DEFAULT NULL,

) ENGINE=InnoDB CHARACTER SET=utf8mb4;



CREATE INDEX items_prefix_id ON items(prefix_id);
CREATE UNIQUE INDEX items_doi ON items(doi(128));
-- resource url not unique sadly
CREATE INDEX items_resource_url ON items(resource_url(128));
CREATE INDEX items_checked_resource_url_unique ON items(checked_resource_url_unique);
CREATE INDEX items_resource_url_unique ON items(resource_url_unique);
CREATE INDEX items_naive_destination_url_updated ON items(naive_destination_url_updated);
CREATE INDEX items_resource_url_domain_id ON items(resource_url_domain_id);
CREATE INDEX items_naive_destination_url ON items(naive_destination_url(512));
CREATE INDEX items_resource_equals_browser_destination_url ON items(h_resource_equals_browser_destination_url);


-- General purpose temporary working table.
create table working_count (
  id INTEGER PRIMARY KEY NOT NULL AUTO_INCREMENT,
  value VARCHAR(2048),
  lowest_id INTEGER,
  count INTEGER NOT NULL DEFAULT 0,
  UNIQUE KEY value (value(128)));

