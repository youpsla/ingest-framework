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
	,lastname VARCHAR(255)   ENCODE lzo
	,job_title VARCHAR(255)   ENCODE lzo
	,company VARCHAR(255)   ENCODE lzo
	,industry VARCHAR(255)   ENCODE lzo
	,hs_analytics_average_page_views VARCHAR(255)   ENCODE lzo
	,contact_owner VARCHAR(255)   ENCODE lzo
	,address VARCHAR(255)   ENCODE lzo
	,zip VARCHAR(255)   ENCODE lzo
	,city VARCHAR(255)   ENCODE lzo
	,region VARCHAR(255)   ENCODE lzo
	,country VARCHAR(255)   ENCODE lzo
	,hs_email_domain VARCHAR(255)   ENCODE lzo
	,hs_analytics_num_event_completions VARCHAR(255)   ENCODE lzo
	,ip_city VARCHAR(255)   ENCODE lzo
	,ip_country VARCHAR(255)   ENCODE lzo
	,ip_country_code VARCHAR(255)   ENCODE lzo
	,ip_state VARCHAR(255)   ENCODE lzo
	,ip_state_code VARCHAR(255)   ENCODE lzo
	,created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,updated_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,portal_id VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;


--DROP TABLE hubspot_development.companies;
CREATE TABLE IF NOT EXISTS hubspot_development.companies
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id VARCHAR(255)   ENCODE lzo
	,domain VARCHAR(255)   ENCODE lzo
	,name VARCHAR(255)   ENCODE lzo
	,hs_analytics_num_page_views VARCHAR(255) ENCODE lzo
	,hs_analytics_num_visits VARCHAR(255) ENCODE lzo
	,hs_is_target_account VARCHAR(255)   ENCODE lzo
	,hs_object_id VARCHAR(255)   ENCODE lzo
	,num_associated_contacts BIGINT ENCODE lzo
	,website VARCHAR(255)   ENCODE lzo
	,hs_parent_company_id VARCHAR(255)   ENCODE lzo
	,portal_id VARCHAR(255)   ENCODE lzo
	
)
DISTSTYLE AUTO
;


--DROP TABLE hubspot_development.events;
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
	,hs_title VARCHAR(2048)   ENCODE lzo
	,hs_city VARCHAR(2048)   ENCODE lzo
	,hs_region VARCHAR(255)   ENCODE lzo
	,hs_country VARCHAR(255)   ENCODE lzo
	,hs_form_id VARCHAR(255)   ENCODE lzo
	,hs_form_correlation_id VARCHAR(255)   ENCODE lzo
	,portal_id VARCHAR(255)   ENCODE lzo
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
	,last_updated_time TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,portal_id VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;


--DROP TABLE hubspot_development.campaign_details;
CREATE TABLE IF NOT EXISTS hubspot_development.campaign_details
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id VARCHAR(255)   ENCODE lzo
	,app_id VARCHAR(255)   ENCODE lzo
	,app_name VARCHAR(255)   ENCODE lzo
	,content_id BIGINT ENCODE lzo
	,subject VARCHAR(255)   ENCODE lzo
	,name VARCHAR(255)   ENCODE lzo
	,n_processed INT ENCODE lzo
	,n_delivered INT ENCODE lzo
	,n_sent INT ENCODE lzo
	,n_open INT ENCODE lzo
	,processing_state VARCHAR(255)   ENCODE lzo
	,type VARCHAR(255)   ENCODE lzo
	,portal_id VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;

--DROP TABLE hubspot_development.email_events;
CREATE TABLE IF NOT EXISTS hubspot_development.email_events
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id VARCHAR(255)   ENCODE lzo
	,recipient VARCHAR(1024)   ENCODE lzo
	,referer VARCHAR(2048)   ENCODE lzo
	,link_id BIGINT   ENCODE lzo
	,url VARCHAR(2048)   ENCODE lzo
	,created TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,app_id VARCHAR(255)   ENCODE lzo
	,country VARCHAR(255)   ENCODE lzo
	,state VARCHAR(255)   ENCODE lzo
	,city VARCHAR(255)   ENCODE lzo
	,type VARCHAR(255)   ENCODE lzo
	,email_campaign_id VARCHAR(255)   ENCODE lzo
	,portal_id VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;

--DROP TABLE hubspot_development.accounts;
CREATE TABLE IF NOT EXISTS hubspot_development.accounts
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,id VARCHAR(255)   ENCODE lzo
	,name VARCHAR(255)   ENCODE lzo
	,portal_id VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;

--DROP TABLE hubspot_development.company_contact_associations;
CREATE TABLE IF NOT EXISTS hubspot_development.company_contact_associations
(
	jab_id INT IDENTITY(1,1)
	,jab_created_at TIMESTAMP WITHOUT TIME ZONE   ENCODE az64 default sysdate
	,company_id VARCHAR(255)   ENCODE lzo
	,contact_id VARCHAR(255)   ENCODE lzo
	,portal_id VARCHAR(255)   ENCODE lzo
)
DISTSTYLE AUTO
;

