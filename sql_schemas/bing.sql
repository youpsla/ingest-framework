-- DROP SCHEMA bing cascade 

CREATE SCHEMA bing;

-- bing.accounts definition

-- DROP TABLE bing.accounts;
CREATE TABLE IF NOT EXISTS bing.accounts
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,Id INTEGER   ENCODE az64
	,Name VARCHAR(255)   ENCODE lzo
	,Number VARCHAR(255)   ENCODE lzo
	,AccountLifeCycleStatus VARCHAR(255)   ENCODE lzo
	,PauseReason VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing.accounts owner to jabmo;



-- DROP TABLE bing.campaigns;
CREATE TABLE IF NOT EXISTS bing.campaigns
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,Id INTEGER   ENCODE az64
	,Name VARCHAR(255)   ENCODE lzo
	,Status VARCHAR(255)   ENCODE lzo
	,CampaignType VARCHAR(255)   ENCODE lzo
	,AccountId INTEGER   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing.campaigns owner to jabmo;


CREATE TABLE IF NOT EXISTS bing.adgroups
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,Id BIGINT   ENCODE az64
	,Name VARCHAR(255)   ENCODE lzo
	,Status VARCHAR(255)   ENCODE lzo
	,CampaignId INTEGER   ENCODE az64
)
DISTSTYLE AUTO
;
ALTER TABLE bing.adgroups owner to jabmo;


CREATE TABLE IF NOT EXISTS bing.ads
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id BIGINT   ENCODE az64
	,type VARCHAR(255)   ENCODE lzo
	,status VARCHAR(255)   ENCODE lzo
	,adgroupid BIGINT   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing.ads owner to jabmo;

CREATE TABLE IF NOT EXISTS bing.demographic_metrics
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
-- ALTER TABLE bing.demographic_metrics owner to jabmo;

CREATE TABLE IF NOT EXISTS bing.geo_metrics
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
-- ALTER TABLE bing.geo_metrics owner to jabmo;



--  Replace <schema_name> with the newly created
 GRANT USAGE ON SCHEMA bing TO GROUP dev_ro;
 GRANT SELECT ON ALL TABLES IN SCHEMA bing TO GROUP dev_ro;
 ALTER DEFAULT PRIVILEGES IN SCHEMA bing GRANT SELECT ON TABLES TO GROUP dev_ro;

 GRANT CREATE ON SCHEMA bing TO GROUP dev_rw;
 GRANT ALL ON ALL TABLES IN SCHEMA bing TO GROUP dev_rw;
 ALTER DEFAULT PRIVILEGES IN SCHEMA bing GRANT ALL ON TABLES TO GROUP dev_rw;

 GRANT CREATE ON DATABASE snowplow TO GROUP dev_rw;
