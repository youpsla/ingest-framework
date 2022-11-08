DB_SECRET_NAMES = {
    "redshift": {
        "development": {
            "readonly": "WEB_INGEST_DEV_RO_SECRET_NAME",
            "readwrite": "WEB_INGEST_DEV_RW_SECRET_NAME",
        },
        "staging": {
            "readonly": "WEB_INGEST_DEV_RO_SECRET_NAME",
            "readwrite": "WEB_INGEST_DEV_RW_SECRET_NAME",
        },
        "prod": {
            "readonly": "WEB_INGEST_PROD_RO_SECRET_NAME",
            "readwrite": "WEB_INGEST_PROD_RW_SECRET_NAME",
        },
    },
}
