-- DROP SCHEMA bing_development cascade;

CREATE SCHEMA hubspot_development;

--DROP TABLE linkedin_staging.creative_text_ads;
CREATE TABLE IF NOT EXISTS hubspot_development.contacts
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER NOT NULL  ENCODE az64
	,email VARCHAR(255)   ENCODE lzo
	,firstname VARCHAR(255)   ENCODE lzo
	,lasr_name VARCHAR(255)   ENCODE lzo
	,created_date VARCHAR(255)   ENCODE lzo
	,last_modified_date VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_staging.creative_text_ads owner to jabmo;