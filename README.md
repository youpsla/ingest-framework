# Ingest
This repository store the first version of the Jabmo home made ETL tool.
It is used for:
- retrieving datas from differents sources
- transforming those datas
- writing to destination (S3, Redshift, ...)

Tasks to be runned are defined in a tasks.json file and data model in a models.json file.

## List of configuration files
- channel.json: Define the list of tasks to be runned
- tasks.json: Define params of each task
- model.json: Define the data model.


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

Example:
```json
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
                            "destination_key": "facet_id"
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
    }
```

#### **response_datas_key**
Type after json.load: str

The name of the key in the response structure which contains datas needed.
THis is done because very often, API return things like "status" or other informations about the request.

#### **base**
Type after json.load: str

The base url.

#### **category**
Type after json.load: str

The part after the base in the url.

TODO: Can be mode in the args list


#### **q**
Type after json.load: str

A specific linkedin kwargs

TODO: Has to be moved in kwargs


#### **params**
Type after json.load: dict

Contains all parameters for buiding urls params.
Also contains params for retrieving datas from Db for inserting in another table during the task run.
Used when one field is used for querying API AND this field data has to be inserted in another table AND this data is not available in the API response.

Keys are on the the following: "db", "dynamics", "statics", constant"

TODO: Has to be moved in kwargs

#### **db**
Type after json.load: dict

A dict containing parameters for collecting required datas from destination (db) to be able to build API query AND self sourced datas (datas from Db).

Example:
```json
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
                "destination_key": "facet_id"
            }
        ],
        "all_fields": [
            "campaign_id"
        ],
        "raw_sql": "select distinct(campaign_id) from {schema}.account_pivot_campaign where start_date between (select date_trunc('month', getdate() - interval '1 month')) and (select last_day(getdate() - interval '1 month'))"
    }
```

#### **type**
Type after json.load: str

It determine if the destination sql query should be done using raw sql or json parameters.

Values can be "db" or "rawsql".


#### **args_fields**
Type after json.load: list

A list of fields that should be added after "base" and "category" in the url. This shopuld be a source table field name.


#### **kwargs_fields**
Type after json.load: list

A list of of lists. Each child list copntains parameters to build kwargs.
[name_of_destination_table_field,template_to_format_to,kwargs_url_key_name]


#### **db_fields**
Type after json.load: list

A list of dict. each dict describe a self sourced field
```python
    "db_fields": [
        {
            "origin_key": name_of_field_in_destination_source_table,
            "destination_key": name_of_field_in_destination_destination_table
        }
```

#### **all_fields**
Type after json.load: list

A list of fields destination table field name. Used to filter retruned result of a query selecting all elemnts from a table for a model


#### **raw_sql**
Type after json.load: str

A raw sql statement to retrieve datas for building endpoint or for inserting in destination.


#### **dynamics**
Contains definition of all kwargs which are dynamic but not collect datas from DB.
It's the case for the date which vary everyday.

It can contains parameters or group of parameters. Each one are dict.

Example:
```json
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
```
At the moment, only dates are managed this way. Then keys are specifics for dates management.

A group can have the following parameters:
- type: If "group", it allow parameters for creating a value which will be used as a source for url_params.
- offset_type: Not used at the moment. Will allow to define the start date before applying substraction defined by below parameters.
- offset_unity: The unit used for "offset_value". Can be "months" or "days".
- offset_value: Number of "offset_unity" to substract from "offset_type".
- offset_range_position: Wether to use the start or the end of the date range defined.
- url_params:
    - type: Which function to apply to the value calculated at the group level.
    - value_type: The type of the value.
    - url_query_parameter_value: THe value to be used as the value for buiding kwarg.
    











