-- DROP SCHEMA bing_development cascade;

CREATE SCHEMA hubspot_development;

--DROP TABLE hubspot_development.contacts;
CREATE TABLE IF NOT EXISTS hubspot_development.contacts
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id VARCHAR(255)   ENCODE lzo
	,email VARCHAR(255)   ENCODE lzo
	,firstname VARCHAR(255)   ENCODE lzo
	,last_name VARCHAR(255)   ENCODE lzo
	,created_date VARCHAR(255)   ENCODE lzo
	,last_modified_date VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;


--DROP TABLE hubspot_development.companies;
CREATE TABLE IF NOT EXISTS hubspot_development.companies
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id VARCHAR(255)   ENCODE lzo
	,createdate VARCHAR(255)   ENCODE lzo
	,domain VARCHAR(255)   ENCODE lzo
	,name VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;


--DROP TABLE hubspot_development.companies;
CREATE TABLE IF NOT EXISTS hubspot_development.events
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id VARCHAR(255)   ENCODE lzo
	,object_id VARCHAR(255)   ENCODE lzo
	,object_type VARCHAR(255)   ENCODE lzo
	,event_type VARCHAR(255)   ENCODE lzo
	,occurred_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,hs_url VARCHAR(2048)   ENCODE lzo
	,hs_title VARCHAR(255)   ENCODE lzo
	,hs_city VARCHAR(255)   ENCODE lzo
	,hs_region VARCHAR(255)   ENCODE lzo
	,hs_country VARCHAR(255)   ENCODE lzo
	,hs_form_id VARCHAR(255)   ENCODE lzo
	,hs_form_correlation_id VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;


--DROP TABLE hubspot_development.campaigns;
CREATE TABLE IF NOT EXISTS hubspot_development.campaigns
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id VARCHAR(255)   ENCODE lzo
	,app_id VARCHAR(255)   ENCODE lzo
	,app_name VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;