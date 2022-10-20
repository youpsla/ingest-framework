-- DROP SCHEMA bing_development cascade;

CREATE SCHEMA bing_production;

-- bing_production.accounts definition

-- DROP TABLE bing_production.accounts;
CREATE TABLE IF NOT EXISTS bing_production.accounts
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id INTEGER   ENCODE az64
	,name VARCHAR(255)   ENCODE lzo
	,number VARCHAR(255)   ENCODE lzo
	,accountLifeCycleStatus VARCHAR(255)   ENCODE lzo
	,pauseReason VARCHAR(255)   ENCODE lzo
	,developer_token VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_production.accounts owner to jabmo;



-- DROP TABLE bing_production.campaigns;
CREATE TABLE IF NOT EXISTS bing_production.campaigns
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
-- ALTER TABLE bing_production.campaigns owner to jabmo;


CREATE TABLE IF NOT EXISTS bing_production.adgroups
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
-- ALTER TABLE bing_production.adgroups owner to jabmo;


CREATE TABLE IF NOT EXISTS bing_production.ads
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id BIGINT   ENCODE az64
	,type VARCHAR(255)   ENCODE lzo
	,status VARCHAR(255)   ENCODE lzo
	,adgroupid BIGINT   ENCODE az64
	,display_url VARCHAR(512)   ENCODE lzo
    ,media_id BIGINT   ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_production.ads owner to jabmo;

CREATE TABLE IF NOT EXISTS bing_production.demographic_metrics
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,time_period  DATE    ENCODE az64
    ,account_id BIGINT   ENCODE az64
    ,campaign_id BIGINT   ENCODE az64
	,adgroup_id BIGINT   ENCODE az64
	,company_name VARCHAR(255)   ENCODE lzo
	,industry_name VARCHAR(255)   ENCODE lzo
	,job_function VARCHAR(255)   ENCODE lzo
	,impressions INT ENCODE az64
	,clicks INT ENCODE az64
	,spend DECIMAL ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_production.demographic_metrics owner to jabmo;

CREATE TABLE IF NOT EXISTS bing_production.user_location_metrics
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,time_period  DATE    ENCODE az64
    ,account_id BIGINT   ENCODE az64
    ,campaign_id BIGINT   ENCODE az64
	,adgroup_id BIGINT   ENCODE az64
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
-- ALTER TABLE bing_production.user_location_metrics owner to jabmo;

CREATE TABLE IF NOT EXISTS bing_production.medias
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id BIGINT   ENCODE az64
	,type VARCHAR(255)   ENCODE lzo
	,url VARCHAR(255)   ENCODE lzo
    ,account_id BIGINT   ENCODE az64	
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_production.medias owner to jabmo;


CREATE TABLE IF NOT EXISTS bing_production.geographic_metrics
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,time_period  DATE    ENCODE az64
    ,account_id BIGINT   ENCODE az64
    ,campaign_id BIGINT   ENCODE az64
	,adgroup_id BIGINT   ENCODE az64
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
-- ALTER TABLE bing_production.geographic_metrics owner to jabmo;


CREATE TABLE IF NOT EXISTS bing_production.ad_metrics
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,time_period  DATE    ENCODE az64
    ,account_id BIGINT   ENCODE az64
    ,campaign_id BIGINT   ENCODE az64
	,adgroup_id BIGINT   ENCODE az64
	,ad_id BIGINT   ENCODE az64
	,impressions INT ENCODE az64
	,clicks INT ENCODE az64
	,spend DECIMAL ENCODE az64
)
DISTSTYLE AUTO
;
-- ALTER TABLE bing_production.ad_metrics owner to jabmo;


--  Replace <schema_name> with the newly created
 GRANT USAGE ON SCHEMA bing_production TO  jabmoro;
 GRANT SELECT ON ALL TABLES IN SCHEMA bing_production TO  jabmoro;
 ALTER DEFAULT PRIVILEGES IN SCHEMA bing_production GRANT SELECT ON TABLES TO  jabmoro;


--  Replace <schema_name> with the newly created
 GRANT USAGE ON SCHEMA bing_production TO GROUP jabmo;
 GRANT SELECT ON ALL TABLES IN SCHEMA bing_production TO GROUP jabmo;
 ALTER DEFAULT PRIVILEGES IN SCHEMA bing_production GRANT SELECT ON TABLES TO GROUP jabmo;

 GRANT CREATE ON SCHEMA bing_production TO GROUP prod_rw;
 GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA bing_production TO GROUP prod_rw;
 ALTER DEFAULT PRIVILEGES IN SCHEMA bing_production GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO GROUP prod_rw;


 create or replace view bing_production.v_geographic_metrics_clicks
            (jab_created_at, time_period, account_id, campaign_id, adgroup_id, country, state, county, city,
             postal_code, metro_area, impressions, clicks, spend, gen_num, id)
as
SELECT t.jab_created_at,
       t.time_period,
       t.account_id,
       t.campaign_id,
       t.adgroup_id,
       t.country,
       t.state,
       t.county,
       t.city,
       t.postal_code,
       t.metro_area,
       t.impressions,
       t.clicks,
       t.spend,
       g.gen_num,
       md5(((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
              t.impressions::character varying::text) || t.postal_code::text) ||
            t.adgroup_id::character varying::text) || t.time_period::character varying::text) AS id
FROM bing_production.geographic_metrics t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
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
                        JOIN (((((((((SELECT 1 AS num
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
                        JOIN (((((((((SELECT 1 AS num
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
                        JOIN (((((((((SELECT 1 AS num
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
                        JOIN (((((((((SELECT 1 AS num
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

alter table bing_production.v_geographic_metrics_clicks
    owner to jabmo;

grant select on bing_production.v_geographic_metrics_clicks to public;

grant select on bing_production.v_geographic_metrics_clicks to jabmoro;


create view bing_production.v_geographic_metrics_impressions
            (jab_created_at, time_period, account_id, campaign_id, adgroup_id, country, state, county, city,
             postal_code, metro_area, impressions, clicks, spend, gen_num, id)
as
SELECT t.jab_created_at,
       t.time_period,
       t.account_id,
       t.campaign_id,
       t.adgroup_id,
       t.country,
       t.state,
       t.county,
       t.city,
       t.postal_code,
       t.metro_area,
       t.impressions,
       t.clicks,
       t.spend,
       g.gen_num,
       md5(((((t.clicks::character varying::text || g.gen_num::character varying::text) ||
              t.impressions::character varying::text) || t.postal_code::text) ||
            t.adgroup_id::character varying::text) || t.time_period::character varying::text) AS id
FROM bing_production.geographic_metrics t
         JOIN (SELECT 10000 * t1.num + 1000 * t2.num + 100 * t3.num + 10 * t4.num + t5.num AS gen_num
               FROM (((((((((SELECT 1 AS num
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
                        JOIN (((((((((SELECT 1 AS num
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
                        JOIN (((((((((SELECT 1 AS num
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
                        JOIN (((((((((SELECT 1 AS num
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
                        JOIN (((((((((SELECT 1 AS num
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

alter table bing_production.v_geographic_metrics_impressions
    owner to jabmo;

grant select on bing_production.v_geographic_metrics_impressions to public;

grant select on bing_production.v_geographic_metrics_impressions to jabmoro;


