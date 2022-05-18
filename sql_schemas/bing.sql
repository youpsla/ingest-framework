
-- DROP SCHEMA bing_development cascade;

CREATE SCHEMA bing_development;

-- bing_development.accounts definition

-- DROP TABLE bing_development.accounts;
CREATE TABLE IF NOT EXISTS bing_development.accounts
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER   ENCODE az64
	,name VARCHAR(255)   ENCODE lzo
	,number VARCHAR(255)   ENCODE lzo
	,accountLifeCycleStatus VARCHAR(255)   ENCODE lzo
	,pauseReason VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.accounts owner to jabmo;



-- DROP TABLE bing_development.campaigns;
CREATE TABLE IF NOT EXISTS bing_development.campaigns
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER   ENCODE az64
	,name VARCHAR(255)   ENCODE lzo
	,status VARCHAR(255)   ENCODE lzo
	,campaignType VARCHAR(255)   ENCODE lzo
	,accountId INTEGER   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.campaigns owner to jabmo;


CREATE TABLE IF NOT EXISTS bing_development.adgroups
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id BIGINT   ENCODE az64
	,name VARCHAR(255)   ENCODE lzo
	,Status VARCHAR(255)   ENCODE lzo
	,CampaignId INTEGER   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.adgroups owner to jabmo;


CREATE TABLE IF NOT EXISTS bing_development.ads
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id BIGINT   ENCODE az64
	,type VARCHAR(255)   ENCODE lzo
	,status VARCHAR(255)   ENCODE lzo
	,adgroupid BIGINT   ENCODE az64
	,final_urls VARCHAR(512)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.ads owner to jabmo;

CREATE TABLE IF NOT EXISTS bing_development.demographic_metrics
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,time_period  DATE    ENCODE az64
	,account_id BIGINT   ENCODE az64
	,account_name VARCHAR(255)   ENCODE lzo
	,campaign_id BIGINT   ENCODE az64
	,adgroup_id BIGINT   ENCODE az64
	,adgroup_name VARCHAR(255)   ENCODE lzo
	,company_name VARCHAR(255)   ENCODE lzo
	,industry_name VARCHAR(255)   ENCODE lzo
	,job_function VARCHAR(255)   ENCODE lzo
	,impressions INT ENCODE az64
	,clicks INT ENCODE az64
	,spend DECIMAL ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.demographic_metrics owner to jabmo;

CREATE TABLE IF NOT EXISTS bing_development.user_location_metrics
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,time_period  DATE    ENCODE az64
	,account_id BIGINT   ENCODE az64
	,account_name VARCHAR(255)   ENCODE lzo
	,adgroup_id BIGINT   ENCODE az64
	,adgroup_name VARCHAR(255)   ENCODE lzo
	,campaign_id BIGINT   ENCODE az64
	,country VARCHAR(255)   ENCODE lzo
	,county VARCHAR(255)   ENCODE lzo
	,state VARCHAR(255)   ENCODE lzo
	,city VARCHAR(255)   ENCODE lzo
	,postal_code VARCHAR(255)   ENCODE lzo
	,metro_area VARCHAR(255)   ENCODE lzo
	,impressions INT ENCODE az64
	,clicks INT ENCODE az64
	,spend DECIMAL ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.user_location_metrics owner to jabmo;

CREATE TABLE IF NOT EXISTS bing_development.medias
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id BIGINT   ENCODE az64
	,type VARCHAR(255)   ENCODE lzo
	,url VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.medias owner to jabmo;


CREATE TABLE IF NOT EXISTS bing_development.media_associations
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,entity_id BIGINT   ENCODE az64
	,media_enabled_entity VARCHAR(255)   ENCODE lzo
	,media_id BIGINT   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.media_associations owner to jabmo;


CREATE TABLE IF NOT EXISTS bing_development.geographic_metrics
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,time_period  DATE    ENCODE az64
	,account_id BIGINT   ENCODE az64
	,account_name VARCHAR(255)   ENCODE lzo
	,adgroup_id BIGINT   ENCODE az64
	,adgroup_name VARCHAR(255)   ENCODE lzo
	,campaign_id BIGINT   ENCODE az64
	,country VARCHAR(255)   ENCODE lzo
	,state VARCHAR(255)   ENCODE lzo
	,county VARCHAR(255)   ENCODE lzo
	,city VARCHAR(255)   ENCODE lzo
	,postal_code VARCHAR(255)   ENCODE lzo
	,metro_area VARCHAR(255)   ENCODE lzo
	,impressions INT ENCODE az64
	,clicks INT ENCODE az64
	,spend DECIMAL ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_development.geographic_metrics owner to jabmo;





--  Replace <schema_name> with the newly created
 GRANT USAGE ON SCHEMA bing_development TO GROUP dev_ro;
 GRANT SELECT ON ALL TABLES IN SCHEMA bing_development TO GROUP dev_ro;
 ALTER DEFAULT PRIVILEGES IN SCHEMA bing_development GRANT SELECT ON TABLES TO GROUP dev_ro;

 GRANT CREATE ON SCHEMA bing_development TO GROUP dev_rw;
 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bing_development TO GROUP dev_rw;
 ALTER DEFAULT PRIVILEGES IN SCHEMA bing_development GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO GROUP dev_rw;

 GRANT CREATE ON DATABASE snowplow TO GROUP dev_rw;
