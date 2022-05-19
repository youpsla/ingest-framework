-- DROP SCHEMA new_linkedin.

CREATE SCHEMA new_linkedin
-- new_linkedin.account_pivot_campaign definition

-- Drop table

-- DROP TABLE new_linkedin.account_pivot_campaign;

--DROP TABLE new_linkedin.account_pivot_campaign;
CREATE TABLE IF NOT EXISTS new_linkedin.account_pivot_campaign
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
ALTER TABLE new_linkedin.account_pivot_campaign owner to jabmo;


-- new_linkedin.accounts definition

-- Drop table

-- DROP TABLE new_linkedin.accounts;

--DROP TABLE new_linkedin.accounts;
CREATE TABLE IF NOT EXISTS new_linkedin.accounts
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
ALTER TABLE new_linkedin.accounts owner to jabmo;


-- new_linkedin.campaign_groups definition

-- Drop table

-- DROP TABLE new_linkedin.campaign_groups;

--DROP TABLE new_linkedin.campaign_groups;
CREATE TABLE IF NOT EXISTS new_linkedin.campaign_groups
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
ALTER TABLE new_linkedin.campaign_groups owner to jabmo;


-- new_linkedin.campaigns definition

-- Drop table

-- DROP TABLE new_linkedin.campaigns;

--DROP TABLE new_linkedin.campaigns;
CREATE TABLE IF NOT EXISTS new_linkedin.campaigns
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
ALTER TABLE new_linkedin.campaigns owner to jabmo;


-- new_linkedin.creative_sponsored_update definition

-- Drop table

-- DROP TABLE new_linkedin.creative_sponsored_update;

--DROP TABLE new_linkedin.creative_sponsored_update;
CREATE TABLE IF NOT EXISTS new_linkedin.creative_sponsored_update
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
ALTER TABLE new_linkedin.creative_sponsored_update owner to jabmo;


-- new_linkedin.creative_sponsored_video definition

-- Drop table

-- DROP TABLE new_linkedin.creative_sponsored_video;

--DROP TABLE new_linkedin.creative_sponsored_video;
CREATE TABLE IF NOT EXISTS new_linkedin.creative_sponsored_video
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
ALTER TABLE new_linkedin.creative_sponsored_video owner to jabmo;


-- new_linkedin.creative_text_ads definition

-- Drop table

-- DROP TABLE new_linkedin.creative_text_ads;

--DROP TABLE new_linkedin.creative_text_ads;
CREATE TABLE IF NOT EXISTS new_linkedin.creative_text_ads
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
ALTER TABLE new_linkedin.creative_text_ads owner to jabmo;


-- new_linkedin.creative_url definition

-- Drop table

-- DROP TABLE new_linkedin.creative_url;

--DROP TABLE new_linkedin.creative_url;
CREATE TABLE IF NOT EXISTS new_linkedin.creative_url
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,creative_id INTEGER NOT NULL  ENCODE az64
	,linkedin_url VARCHAR(5000)   ENCODE lzo
	,preview_url VARCHAR(5000)   ENCODE lzo
)
DISTSTYLE AUTO
;
ALTER TABLE new_linkedin.creative_url owner to jabmo;


-- new_linkedin.pivot_creative definition

-- Drop table

-- DROP TABLE new_linkedin.pivot_creative;

--DROP TABLE new_linkedin.pivot_creative;
CREATE TABLE IF NOT EXISTS new_linkedin.pivot_creative
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
ALTER TABLE new_linkedin.pivot_creative owner to jabmo;


-- new_linkedin.pivot_job_title definition

-- Drop table

-- DROP TABLE new_linkedin.pivot_job_title;

--DROP TABLE new_linkedin.pivot_job_title;
CREATE TABLE IF NOT EXISTS new_linkedin.pivot_job_title
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
ALTER TABLE new_linkedin.pivot_job_title owner to jabmo;


-- new_linkedin.pivot_member_company definition

-- Drop table

-- DROP TABLE new_linkedin.pivot_member_company;

--DROP TABLE new_linkedin.pivot_member_company;
CREATE TABLE IF NOT EXISTS new_linkedin.pivot_member_company
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
ALTER TABLE new_linkedin.pivot_member_company owner to jabmo;


-- new_linkedin.pivot_member_country definition

-- Drop table

-- DROP TABLE new_linkedin.pivot_member_country;

--DROP TABLE new_linkedin.pivot_member_country;
CREATE TABLE IF NOT EXISTS new_linkedin.pivot_member_country
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
ALTER TABLE new_linkedin.pivot_member_country owner to jabmo;


-- new_linkedin.social_metrics definition

-- Drop table

-- DROP TABLE new_linkedin.social_metrics;

--DROP TABLE new_linkedin.social_metrics;
CREATE TABLE IF NOT EXISTS new_linkedin.social_metrics
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
ALTER TABLE new_linkedin.social_metrics owner to jabmo;



-- new_linkedin.v_pivot_member_country_clicks source

CREATE OR REPLACE VIEW new_linkedin.v_pivot_member_country_clicks
AS SELECT t.geo_id, t.impressions, t.clicks, t.cost_in_usd, t.facet, t.facet_id, t.country, t.time_granularity, t.start_date, t.end_date, t.jab_created_at, g.gen_num, md5((((((t.clicks::character varying::text || g.gen_num::character varying::text) || t.impressions::character varying::text) || t.geo_id::character varying::text) || t.facet_id::character varying::text) || t.start_date::character varying::text) || t.geo_id::character varying::text) AS id
   FROM new_linkedin.pivot_member_country t
   JOIN ( SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
           FROM ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t1
      JOIN ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t2 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
   UNION 
            SELECT 2)
   UNION 
            SELECT 3)
   UNION 
            SELECT 4)
   UNION 
            SELECT 5)
   UNION 
            SELECT 6)
   UNION 
            SELECT 7)
   UNION 
            SELECT 8)
   UNION 
            SELECT 9)
   UNION 
            SELECT 0) t3 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
       SELECT 2)
UNION 
       SELECT 3)
UNION 
       SELECT 4)
UNION 
       SELECT 5)
UNION 
       SELECT 6)
UNION 
       SELECT 7)
UNION 
       SELECT 8)
UNION 
       SELECT 9)
UNION 
       SELECT 0) t4 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
  SELECT 2)
UNION 
  SELECT 3)
UNION 
  SELECT 4)
UNION 
  SELECT 5)
UNION 
  SELECT 6)
UNION 
  SELECT 7)
UNION 
  SELECT 8)
UNION 
  SELECT 9)
UNION 
  SELECT 0) t5 ON 1 = 1
  WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
  ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
  WHERE g.gen_num <= t.clicks;

-- Permissions

GRANT SELECT ON TABLE new_linkedin.v_pivot_member_country_clicks TO public;
GRANT UPDATE, RULE, DELETE, SELECT, REFERENCES, TRIGGER, INSERT ON TABLE new_linkedin.v_pivot_member_country_clicks TO jabmo;
GRANT SELECT ON TABLE new_linkedin.v_pivot_member_country_clicks TO jabmoro;





-- new_linkedin.v_social_metrics_clicks source

CREATE OR REPLACE VIEW new_linkedin.v_social_metrics_clicks
AS SELECT t.creative_id, t.campaign_id, t.advertiser_id, t.costinusd, t.start_date, t.end_date, t.impressions, t.shares, t.clicks, t.likes, t.status, t.creative_type, t.time_granularity, g.gen_num, md5((((t.clicks::character varying::text || g.gen_num::character varying::text) || t.impressions::character varying::text) || t.start_date::character varying::text) || t.creative_id::character varying::text) AS id
   FROM new_linkedin.social_metrics t
   JOIN ( SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
           FROM ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t1
      JOIN ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t2 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
   UNION 
            SELECT 2)
   UNION 
            SELECT 3)
   UNION 
            SELECT 4)
   UNION 
            SELECT 5)
   UNION 
            SELECT 6)
   UNION 
            SELECT 7)
   UNION 
            SELECT 8)
   UNION 
            SELECT 9)
   UNION 
            SELECT 0) t3 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
       SELECT 2)
UNION 
       SELECT 3)
UNION 
       SELECT 4)
UNION 
       SELECT 5)
UNION 
       SELECT 6)
UNION 
       SELECT 7)
UNION 
       SELECT 8)
UNION 
       SELECT 9)
UNION 
       SELECT 0) t4 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
  SELECT 2)
UNION 
  SELECT 3)
UNION 
  SELECT 4)
UNION 
  SELECT 5)
UNION 
  SELECT 6)
UNION 
  SELECT 7)
UNION 
  SELECT 8)
UNION 
  SELECT 9)
UNION 
  SELECT 0) t5 ON 1 = 1
  WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
  ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
  WHERE g.gen_num <= t.clicks;

-- Permissions

GRANT SELECT ON TABLE new_linkedin.v_social_metrics_clicks TO public;
GRANT UPDATE, RULE, DELETE, SELECT, REFERENCES, TRIGGER, INSERT ON TABLE new_linkedin.v_social_metrics_clicks TO jabmo;
GRANT SELECT ON TABLE new_linkedin.v_social_metrics_clicks TO jabmoro;





-- new_linkedin.v_social_metrics_impressions source

CREATE OR REPLACE VIEW new_linkedin.v_social_metrics_impressions
AS SELECT t.creative_id, t.campaign_id, t.advertiser_id, t.costinusd, t.start_date, t.end_date, t.impressions, t.shares, t.clicks, t.likes, t.status, t.creative_type, t.time_granularity, g.gen_num, md5((((t.clicks::character varying::text || g.gen_num::character varying::text) || t.impressions::character varying::text) || t.start_date::character varying::text) || t.creative_id::character varying::text) AS id
   FROM new_linkedin.social_metrics t
   JOIN ( SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
           FROM ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t1
      JOIN ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t2 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
   UNION 
            SELECT 2)
   UNION 
            SELECT 3)
   UNION 
            SELECT 4)
   UNION 
            SELECT 5)
   UNION 
            SELECT 6)
   UNION 
            SELECT 7)
   UNION 
            SELECT 8)
   UNION 
            SELECT 9)
   UNION 
            SELECT 0) t3 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
       SELECT 2)
UNION 
       SELECT 3)
UNION 
       SELECT 4)
UNION 
       SELECT 5)
UNION 
       SELECT 6)
UNION 
       SELECT 7)
UNION 
       SELECT 8)
UNION 
       SELECT 9)
UNION 
       SELECT 0) t4 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
  SELECT 2)
UNION 
  SELECT 3)
UNION 
  SELECT 4)
UNION 
  SELECT 5)
UNION 
  SELECT 6)
UNION 
  SELECT 7)
UNION 
  SELECT 8)
UNION 
  SELECT 9)
UNION 
  SELECT 0) t5 ON 1 = 1
  WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
  ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
  WHERE g.gen_num <= t.impressions;

-- Permissions

GRANT SELECT ON TABLE new_linkedin.v_social_metrics_impressions TO public;
GRANT UPDATE, RULE, DELETE, SELECT, REFERENCES, TRIGGER, INSERT ON TABLE new_linkedin.v_social_metrics_impressions TO jabmo;
GRANT SELECT ON TABLE new_linkedin.v_social_metrics_impressions TO jabmoro;



-- new_linkedin.v_social_metrics_likes source

CREATE OR REPLACE VIEW new_linkedin.v_social_metrics_likes
AS SELECT t.creative_id, t.campaign_id, t.advertiser_id, t.costinusd, t.start_date, t.end_date, t.impressions, t.shares, t.clicks, t.likes, t.status, t.creative_type, t.time_granularity, g.gen_num, md5((((t.clicks::character varying::text || g.gen_num::character varying::text) || t.impressions::character varying::text) || t.start_date::character varying::text) || t.creative_id::character varying::text) AS id
   FROM new_linkedin.social_metrics t
   JOIN ( SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
           FROM ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t1
      JOIN ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t2 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
   UNION 
            SELECT 2)
   UNION 
            SELECT 3)
   UNION 
            SELECT 4)
   UNION 
            SELECT 5)
   UNION 
            SELECT 6)
   UNION 
            SELECT 7)
   UNION 
            SELECT 8)
   UNION 
            SELECT 9)
   UNION 
            SELECT 0) t3 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
       SELECT 2)
UNION 
       SELECT 3)
UNION 
       SELECT 4)
UNION 
       SELECT 5)
UNION 
       SELECT 6)
UNION 
       SELECT 7)
UNION 
       SELECT 8)
UNION 
       SELECT 9)
UNION 
       SELECT 0) t4 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
  SELECT 2)
UNION 
  SELECT 3)
UNION 
  SELECT 4)
UNION 
  SELECT 5)
UNION 
  SELECT 6)
UNION 
  SELECT 7)
UNION 
  SELECT 8)
UNION 
  SELECT 9)
UNION 
  SELECT 0) t5 ON 1 = 1
  WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
  ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
  WHERE g.gen_num <= t.likes;

-- Permissions

GRANT SELECT ON TABLE new_linkedin.v_social_metrics_likes TO public;
GRANT UPDATE, RULE, DELETE, SELECT, REFERENCES, TRIGGER, INSERT ON TABLE new_linkedin.v_social_metrics_likes TO jabmo;
GRANT SELECT ON TABLE new_linkedin.v_social_metrics_likes TO jabmoro;



-- new_linkedin.v_social_metrics_shares source

CREATE OR REPLACE VIEW new_linkedin.v_social_metrics_shares
AS SELECT t.creative_id, t.campaign_id, t.advertiser_id, t.costinusd, t.start_date, t.end_date, t.impressions, t.shares, t.clicks, t.likes, t.status, t.creative_type, t.time_granularity, g.gen_num, md5((((t.clicks::character varying::text || g.gen_num::character varying::text) || t.impressions::character varying::text) || t.start_date::character varying::text) || t.creative_id::character varying::text) AS id
   FROM new_linkedin.social_metrics t
   JOIN ( SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
           FROM ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t1
      JOIN ((((((((( SELECT 1 AS num
        UNION 
                 SELECT 2)
        UNION 
                 SELECT 3)
        UNION 
                 SELECT 4)
        UNION 
                 SELECT 5)
        UNION 
                 SELECT 6)
        UNION 
                 SELECT 7)
        UNION 
                 SELECT 8)
        UNION 
                 SELECT 9)
        UNION 
                 SELECT 0) t2 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
   UNION 
            SELECT 2)
   UNION 
            SELECT 3)
   UNION 
            SELECT 4)
   UNION 
            SELECT 5)
   UNION 
            SELECT 6)
   UNION 
            SELECT 7)
   UNION 
            SELECT 8)
   UNION 
            SELECT 9)
   UNION 
            SELECT 0) t3 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
       SELECT 2)
UNION 
       SELECT 3)
UNION 
       SELECT 4)
UNION 
       SELECT 5)
UNION 
       SELECT 6)
UNION 
       SELECT 7)
UNION 
       SELECT 8)
UNION 
       SELECT 9)
UNION 
       SELECT 0) t4 ON 1 = 1
   JOIN ((((((((( SELECT 1 AS num
UNION 
  SELECT 2)
UNION 
  SELECT 3)
UNION 
  SELECT 4)
UNION 
  SELECT 5)
UNION 
  SELECT 6)
UNION 
  SELECT 7)
UNION 
  SELECT 8)
UNION 
  SELECT 9)
UNION 
  SELECT 0) t5 ON 1 = 1
  WHERE (10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) > 0
  ORDER BY 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num) g ON 1 = 1
  WHERE g.gen_num <= t.shares;

-- Permissions

GRANT SELECT ON TABLE new_linkedin.v_social_metrics_shares TO public;
GRANT UPDATE, RULE, DELETE, SELECT, REFERENCES, TRIGGER, INSERT ON TABLE new_linkedin.v_social_metrics_shares TO jabmo;
GRANT SELECT ON TABLE new_linkedin.v_social_metrics_shares TO jabmoro;

