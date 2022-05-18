delete from  bing.demographic_metrics;
delete from  bing.user_location_metrics;
delete from  bing.geographic_metrics;
delete from bing.campaigns;
delete from bing.ads;
delete from bing.adgroups;
delete from bing.medias;



delete from bing_development.campaigns;
delete from bing_development.adgroups;
delete from bing_development.medias;
delete from bing_development.ads;
delete from bing_development.demographic_metrics;
delete from bing_development.user_location_metrics;
delete from bing_development.geographic_metrics;
delete from bing_development.media_associations ;


copy bing.demographic_metrics (time_period, account_id, account_name, campaign_id, adgroup_id, adgroup_name, company_name, industry_name, job_function, impressions, clicks, spend) from 's3://linkedin-ingest-dev-staging/application=ingest/channel=bing/environment=staging/state=unprocessed/task=daily_demographic_metrics_update' iam_role 'arn:aws:iam::467882466042:role/aa-ingest-dev-redshift-s3' csv IGNOREHEADER 1 FILLRECORD;