S3 storage path:

<s3_bucket_name>:
    - "production"
    - "development"
    - "staging"
        - "tmp"
        - "datas"
            - <YYYY>
                - <MM>
                    - <DD>
                        - <task_name>
                            - xxx.parquet (xxx can be a number or we can put all infromatiosn in filename)
                            - ...

