# Ingest
This repository store the first version of the Jabmo home made ETL tool.
It is used for:
- retrieving datas from differents sources
- transforming those datas
- writing to destination (S3, Redshift, ...)

Tasks to be runned are defined in a tasks.json file and data models in a models.json file.

## Task.json
This file contains a list of tasks. Here is an example of task:

```json
        "pivot_job_title_monthly_update": {
            "actions": [
                "insert"
            ],
            "exclude_existing_in_db": false,
            "source": "linkedin",
            "response_datas_key": "elements",
            "destination": "redshift",
            "model": "pivot_job_title",
            "url": {
                "response_datas_key": "elements",
                "base": "https://api.linkedin.com/v2/",
                "category": "adAnalyticsV2",
                "q": "analytics",
                "params": {
                    "db": {
                        "_": {
                            "type": "db rawsql",
                            "args_fields": [],
                            "kwargs_fields": [
                                [
                                    "campaign_id",
                                    "urn:li:sponsoredCampaign:{}",
                                    "campaigns[0]"
                                ]
                            ],
                            "db_fields": [
                                {
                                    "origin_key": "campaign_id",
                                    "destintation_key": "facet_id"
                                }
                            ],
                            "all_fields": [
                                "campaign_id"
                            ],
                            "raw_sql": "select distinct(campaign_id) from {schema}.account_pivot_campaign where start_date between (select date_trunc('month', getdate() - interval '1 month')) and (select last_day(getdate() - interval '1 month'))"
                        }
                    },
                    "dynamics": {
                        "start_date": {
                            "type": "group",
                            "offset_type": "from_today",
                            "offset_unity": "months",
                            "offset_value": "1",
                            "offset_range_position": "start_day",
                            "url_params": {
                                "dateRange.start.day": {
                                    "type": "filter_by",
                                    "value_type": "date",
                                    "url_query_parameter_value": "{day}",
                                    "offset": ""
                                },
                                "dateRange.start.month": {
                                    "type": "filter_by",
                                    "value_type": "date",
                                    "url_query_parameter_value": "{month}"
                                },
                                "dateRange.start.year": {
                                    "type": "filter_by",
                                    "value_type": "date",
                                    "url_query_parameter_value": "{year}"
                                }
                            }
                        },
                        "end_date": {
                            "type": "group",
                            "offset_type": "from_today",
                            "offset_unity": "months",
                            "offset_value": "1",
                            "offset_range_position": "end_day",
                            "url_params": {
                                "dateRange.end.day": {
                                    "type": "filter_by",
                                    "value_type": "date",
                                    "url_query_parameter_value": "{day}",
                                    "offset": ""
                                },
                                "dateRange.end.month": {
                                    "type": "filter_by",
                                    "value_type": "date",
                                    "url_query_parameter_value": "{month}"
                                },
                                "dateRange.end.year": {
                                    "type": "filter_by",
                                    "value_type": "date",
                                    "url_query_parameter_value": "{year}"
                                }
                            }
                        }
                    },
                    "statics": {
                        "timeGranularity": "MONTHLY",
                        "pivot": "MEMBER_JOB_TITLE",
                        "fields": "externalWebsiteConversions,dateRange,impressions,costInUsd,clicks,shares,costInLocalCurrency,pivot,pivotValue",
                        "projection": "(*,elements*(externalWebsiteConversions,dateRange(*),impressions,costInUsd,clicks,shares,costInLocalCurrency,pivot,pivotValue~))"
                    }
                }
            },
            "request": {
                "header": null
            }
        }
```

### Task.json availables parameters

#### **name**
Type after json.load: str

The name of the task.

#### **source**
Type after json.load: str

The name of the source of the data. Can be "linkedin", "boing", ...

#### **destination**
Type after json.load: str

The name of the destination of the data. Can be "s3", "redshift", ...

#### **model**
Type after json.load: str

The name of the model of data. At the moment, it's the name of the table which is the target of datas.

At the moment, it's not possible to insert datas in differents table when running a task. If you need that, define more than one task.

#### **exclude_existing_in_db**
Type after json.load: boolean

If set to True, the sysem will try to lookup in destination datas if a record with the same value for the already exists. The value of the "exclude_key" task parameter is used to compare.

#### **exclude_key**
Type after json.load: str

The name of the key to use to filter for already existing records.

#### **url**
Type after json.load: dict

Contains all parameters necessary to build the endpoint query.




