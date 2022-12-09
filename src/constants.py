ENVS_LIST = ["development", "test", "staging", "production"]

REDSHIT_S3_ARN = "arn:aws:iam::467882466042:role/aa-ingest-dev-redshift-s3"

DB_SECRET_NAMES = {
    "redshift": {
        "development": {
            "readonly": "jabmo/db/redshift/web-ingest/dev/ro",
            "readwrite": "jabmo/db/redshift/web-ingest/dev/rw",
        },
        "staging": {
            "readonly": "jabmo/db/redshift/web-ingest/prod/ro",
            "readwrite": "jabmo/db/redshift/web-ingest/prod/rw",
        },
        "production": {
            "readonly": "jabmo/db/redshift/web-ingest/prod",
            "readwrite": "jabmo/db/redshift/web-ingest/prod/rw",
        },
    },
}
