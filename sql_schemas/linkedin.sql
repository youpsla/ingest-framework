-- DROP SCHEMA linkedin_development.

-- CREATE SCHEMA linkedin_development
-- linkedin_development.account_pivot_campaign definition

-- Drop table

-- DROP TABLE linkedin_development.account_pivot_campaign;

--DROP TABLE linkedin_development.account_pivot_campaign;
CREATE TABLE IF NOT EXISTS linkedin_development.account_pivot_campaign
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,advertiser_id INTEGER NOT NULL  ENCODE az64
	,campaign_id INTEGER NOT NULL  ENCODE az64
	,campaign_name VARCHAR(255)   ENCODE lzo
	,campaign_status VARCHAR(255)   ENCODE lzo
	,campaign_group_id INTEGER NOT NULL  ENCODE az64
	,impressions INTEGER NOT NULL  ENCODE az64
	,clicks INTEGER NOT NULL  ENCODE az64
	,shares INTEGER NOT NULL  ENCODE az64
	,costinusd VARCHAR(50) NOT NULL  ENCODE lzo
	,ad_format VARCHAR(255)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,entry_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.account_pivot_campaign owner to jabmo;


-- linkedin_development.accounts definition

-- Drop table

-- DROP TABLE linkedin_development.accounts;

--DROP TABLE linkedin_development.accounts;
CREATE TABLE IF NOT EXISTS linkedin_development.accounts
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER NOT NULL  ENCODE az64
	,name VARCHAR(255) NOT NULL  ENCODE lzo
	,currency VARCHAR(50) NOT NULL  ENCODE lzo
	,account_type VARCHAR(50) NOT NULL  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,status VARCHAR(50) NOT NULL  ENCODE lzo
	,ref VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.accounts owner to jabmo;


-- linkedin_development.campaign_groups definition

-- Drop table

-- DROP TABLE linkedin_development.campaign_groups;

--DROP TABLE linkedin_development.campaign_groups;
CREATE TABLE IF NOT EXISTS linkedin_development.campaign_groups
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER NOT NULL  ENCODE az64
	,name VARCHAR(255) NOT NULL  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,account_id INTEGER NOT NULL  ENCODE az64
	,status VARCHAR(50) NOT NULL  ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.campaign_groups owner to jabmo;


-- linkedin_development.campaigns definition

-- Drop table

-- DROP TABLE linkedin_development.campaigns;

--DROP TABLE linkedin_development.campaigns;
CREATE TABLE IF NOT EXISTS linkedin_development.campaigns
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER NOT NULL  ENCODE az64
	,name VARCHAR(255) NOT NULL  ENCODE lzo
	,campaign_group_id INTEGER NOT NULL  ENCODE az64
	,account_id INTEGER NOT NULL  ENCODE az64
	,associated_entity VARCHAR(255) NOT NULL  ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,last_modified_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,status VARCHAR(50) NOT NULL  ENCODE lzo
	,cost_type VARCHAR(15) NOT NULL  ENCODE lzo
	,unit_cost_currency_code VARCHAR(15) NOT NULL  ENCODE lzo
	,unit_cost_amount VARCHAR(15) NOT NULL  ENCODE lzo
	,daily_budget_currency_code VARCHAR(15)   ENCODE lzo
	,daily_budget_amount VARCHAR(15)   ENCODE lzo
	,ad_format VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.campaigns owner to jabmo;


-- linkedin_development.creative_sponsored_update definition

-- Drop table

-- DROP TABLE linkedin_development.creative_sponsored_update;

--DROP TABLE linkedin_development.creative_sponsored_update;
CREATE TABLE IF NOT EXISTS linkedin_development.creative_sponsored_update
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,creative_id INTEGER   ENCODE az64
	,title VARCHAR(800)   ENCODE lzo
	,text VARCHAR(5000)   ENCODE lzo
	,creative_url VARCHAR(5000)   ENCODE lzo
	,description VARCHAR(5000)   ENCODE lzo
	,subject VARCHAR(800)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.creative_sponsored_update owner to jabmo;


-- linkedin_development.creative_sponsored_video definition

-- Drop table

-- DROP TABLE linkedin_development.creative_sponsored_video;

--DROP TABLE linkedin_development.creative_sponsored_video;
CREATE TABLE IF NOT EXISTS linkedin_development.creative_sponsored_video
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,creative_id INTEGER NOT NULL  ENCODE az64
	,campaign_id INTEGER NOT NULL  ENCODE az64
	,creative_name VARCHAR(255)   ENCODE lzo
	,ugc_ref VARCHAR(255)   ENCODE lzo
	,status VARCHAR(255)   ENCODE lzo
	,duration_video INTEGER   ENCODE az64
	,created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,width VARCHAR(50)   ENCODE lzo
	,height VARCHAR(50)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.creative_sponsored_video owner to jabmo;


-- linkedin_development.creative_text_ads definition

-- Drop table

-- DROP TABLE linkedin_development.creative_text_ads;

--DROP TABLE linkedin_development.creative_text_ads;
CREATE TABLE IF NOT EXISTS linkedin_development.creative_text_ads
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER NOT NULL  ENCODE az64
	,title VARCHAR(255)   ENCODE lzo
	,text VARCHAR(255)   ENCODE lzo
	,vector_image VARCHAR(255)   ENCODE lzo
	,click_uri VARCHAR(255)   ENCODE lzo
	,"type" VARCHAR(255)   ENCODE lzo
	,status VARCHAR(255)   ENCODE lzo
	,created_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,modified_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,entry_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.creative_text_ads owner to jabmo;


-- linkedin_development.creative_url definition

-- Drop table

-- DROP TABLE linkedin_development.creative_url;

--DROP TABLE linkedin_development.creative_url;
CREATE TABLE IF NOT EXISTS linkedin_development.creative_url
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,creative_id INTEGER NOT NULL  ENCODE az64
	,linkedin_url VARCHAR(5000)   ENCODE lzo
	,preview_url VARCHAR(5000)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.creative_url owner to jabmo;


-- linkedin_development.pivot_creative definition

-- Drop table

-- DROP TABLE linkedin_development.pivot_creative;

--DROP TABLE linkedin_development.pivot_creative;
CREATE TABLE IF NOT EXISTS linkedin_development.pivot_creative
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,creative_id INTEGER NOT NULL  ENCODE az64
	,impressions INTEGER NOT NULL  ENCODE az64
	,clicks INTEGER NOT NULL  ENCODE az64
	,cost_in_usd VARCHAR(50) NOT NULL  ENCODE lzo
	,facet VARCHAR(50) NOT NULL  ENCODE lzo
	,facet_id INTEGER NOT NULL  ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,time_granularity VARCHAR(255)   ENCODE lzo
	,account_id INTEGER   ENCODE az64
	,creative_type VARCHAR(255)   ENCODE lzo
	,status VARCHAR(50)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.pivot_creative owner to jabmo;


-- linkedin_development.pivot_job_title definition

-- Drop table

-- DROP TABLE linkedin_development.pivot_job_title;

--DROP TABLE linkedin_development.pivot_job_title;
CREATE TABLE IF NOT EXISTS linkedin_development.pivot_job_title
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER NOT NULL  ENCODE az64
	,impressions INTEGER NOT NULL  ENCODE az64
	,clicks INTEGER NOT NULL  ENCODE az64
	,cost_in_usd VARCHAR(50) NOT NULL  ENCODE lzo
	,facet VARCHAR(50) NOT NULL  ENCODE lzo
	,facet_id INTEGER NOT NULL  ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,time_granularity VARCHAR(255)   ENCODE lzo
	,job_title VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
 SORTKEY (
	facet_id
	)
;
-- ALTER TABLE linkedin_development.pivot_job_title owner to jabmo;


-- linkedin_development.pivot_member_company definition

-- Drop table

-- DROP TABLE linkedin_development.pivot_member_company;

--DROP TABLE linkedin_development.pivot_member_company;
CREATE TABLE IF NOT EXISTS linkedin_development.pivot_member_company
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,organization_id INTEGER NOT NULL  ENCODE az64
	,impressions INTEGER NOT NULL  ENCODE az64
	,clicks INTEGER NOT NULL  ENCODE az64
	,cost_in_usd VARCHAR(50) NOT NULL  ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,facet VARCHAR(50) NOT NULL  ENCODE lzo
	,facet_id INTEGER NOT NULL  ENCODE az64
	,organization_name VARCHAR(255)   ENCODE lzo
	,time_granularity VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.pivot_member_company owner to jabmo;


-- linkedin_development.pivot_member_country definition

-- Drop table

-- DROP TABLE linkedin_development.pivot_member_country;

--DROP TABLE linkedin_development.pivot_member_country;
CREATE TABLE IF NOT EXISTS linkedin_development.pivot_member_country
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,geo_id INTEGER NOT NULL  ENCODE az64
	,impressions INTEGER NOT NULL  ENCODE az64
	,clicks INTEGER NOT NULL  ENCODE az64
	,cost_in_usd VARCHAR(50) NOT NULL  ENCODE lzo
	,facet VARCHAR(50) NOT NULL  ENCODE lzo
	,facet_id INTEGER NOT NULL  ENCODE az64
	,country VARCHAR(255)   ENCODE lzo
	,time_granularity VARCHAR(255)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,"region" VARCHAR(255)   ENCODE lzo
	,city VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.pivot_member_country owner to jabmo;


-- linkedin_development.pivot_job_title_full definition

-- Drop table

-- DROP TABLE linkedin_development.pivot_job_title_full;

--DROP TABLE linkedin_development.pivot_job_title_full;
CREATE TABLE IF NOT EXISTS linkedin_development.pivot_job_title_full
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER NOT NULL  ENCODE az64
	,impressions INTEGER NOT NULL  ENCODE az64
	,clicks INTEGER NOT NULL  ENCODE az64
	,cost_in_usd VARCHAR(50) NOT NULL  ENCODE lzo
	,facet VARCHAR(50) NOT NULL  ENCODE lzo
	,facet_id INTEGER NOT NULL  ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,time_granularity VARCHAR(255)   ENCODE lzo
	,job_title VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
 SORTKEY (
	facet_id
	)
;
--ALTER TABLE linkedin_development.pivot_job_title_full owner to jabmo;


-- linkedin_development.pivot_member_company_full definition

-- Drop table

-- DROP TABLE linkedin_development.pivot_member_company_full;

--DROP TABLE linkedin_development.pivot_member_company_full;
CREATE TABLE IF NOT EXISTS linkedin_development.pivot_member_company_full
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,organization_id INTEGER NOT NULL  ENCODE az64
	,impressions INTEGER NOT NULL  ENCODE az64
	,clicks INTEGER NOT NULL  ENCODE az64
	,cost_in_usd VARCHAR(50) NOT NULL  ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL  ENCODE az64
	,facet VARCHAR(50) NOT NULL  ENCODE lzo
	,facet_id INTEGER NOT NULL  ENCODE az64
	,organization_name VARCHAR(255)   ENCODE lzo
	,time_granularity VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
--ALTER TABLE linkedin_development.pivot_member_company owner to jabmo;


-- linkedin_development.pivot_member_country_full definition

-- Drop table

-- DROP TABLE linkedin_development.pivot_member_country_full;

--DROP TABLE linkedin_development.pivot_member_country_full;
CREATE TABLE IF NOT EXISTS linkedin_development.pivot_member_country_full
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,geo_id INTEGER NOT NULL  ENCODE az64
	,impressions INTEGER NOT NULL  ENCODE az64
	,clicks INTEGER NOT NULL  ENCODE az64
	,cost_in_usd VARCHAR(50) NOT NULL  ENCODE lzo
	,facet VARCHAR(50) NOT NULL  ENCODE lzo
	,facet_id INTEGER NOT NULL  ENCODE az64
	,country VARCHAR(255)   ENCODE lzo
	,time_granularity VARCHAR(255)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,"region" VARCHAR(255)   ENCODE lzo
	,city VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.pivot_member_country_full owner to jabmo;






-- linkedin_development.social_metrics definition

-- Drop table

-- DROP TABLE linkedin_development.social_metrics;

--DROP TABLE linkedin_development.social_metrics;
CREATE TABLE IF NOT EXISTS linkedin_development.social_metrics
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,creative_id INTEGER   ENCODE az64
	,campaign_id INTEGER   ENCODE az64
	,advertiser_id INTEGER   ENCODE az64
	,costinusd NUMERIC(18,0)   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,impressions INTEGER   ENCODE az64
	,shares INTEGER   ENCODE az64
	,clicks INTEGER   ENCODE az64
	,likes INTEGER   ENCODE az64
	,status VARCHAR(255)   ENCODE lzo
	,creative_type VARCHAR(255)   ENCODE lzo
	,time_granularity VARCHAR(50)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE linkedin_development.social_metrics owner to jabmo;