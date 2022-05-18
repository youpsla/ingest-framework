import os
from datetime import datetime
from io import BytesIO

import boto3
from botocore.exceptions import ClientError
from src.clients.bing.bing_client import FILE_DIRECTORY
from src.clients.s3.s3wrappers import ObjectWrapper
from src.commons.client import Client
from src.constants import PROCESSED_STATE, REDSHIT_S3_ARN, UNPROCESSED_STATE
from src.utils.custom_logger import logger
from src.utils.sql_utils import SqlQuery
from src.utils.various_utils import get_schema_name


class S3Client(Client):
    def __init__(
        self,
        application=None,
        task=None,
        bucket_name=None,
        env="development",
        date=None,
        state_target=UNPROCESSED_STATE,
    ):
        super().__init__(env)
        self.client = boto3.client("s3")
        self.resource = boto3.resource("s3")
        self.application = application
        self.task = task
        self.bucket_name = "linkedin-ingest-dev-staging"
        self.date = date
        self._task_full_path = None
        self._bucket = None
        self.tmp_objects_list = []
        self.running_dt = datetime.now()
        self.state_target = state_target

    @property
    def bucket(self):
        if not self._bucket:
            self._bucket = self.resource.Bucket(self.bucket_name)
        return self._bucket

    def upload_and_delete_source(self):
        self.upload_from_file_object()

    def get_object_key_path_elements(self):
        """Build list of tuples allowing to build the path.



        Args:
            date: Optionnal. A datetime object.
            Default to None

        Returns:
            A list of tuples: ((data_name, data_value), ...)
        """
        result = []

        # Add elements
        # TODO: Use application instead of constant
        # result.append(("application", self.application.name))
        result.append(("application", "ingest"))
        result.append(("channel", self.task.params["source"]))
        result.append(("environment", self.env))
        result.append(("state", self.state_target))
        result.append(("task", self.task.name))
        result.append(("year", f"{self.running_dt:%Y}"))
        result.append(("month", f"{self.running_dt:%m}"))
        result.append(("day", f"{self.running_dt:%d}"))
        result.append(("hour", f"{self.running_dt:%H}"))
        result.append(("minute", f"{self.running_dt:%M}"))
        result.append(("second", f"{self.running_dt:%S}"))

        return result

    def get_unprocessed_objects(self):
        objects = self.bucket.objects.filter(
            Prefix="application=ingest/channel={channel}/environment={environment}/state={state}/task={task}".format(
                channel=self.task.channel,
                environment=os.environ["RUNNING_ENV"],
                state=UNPROCESSED_STATE,
                task=self.task.params["s3_source_task"],
            )
        )
        return objects

    def copy_to_redshift_and_delete(self):
        self.copy_to_redshift()
        objects = self.get_unprocessed_objects()
        for o in objects:
            old_key = o.key
            new_key = old_key.replace(UNPROCESSED_STATE, PROCESSED_STATE)
            self.client.copy_object(
                Bucket=self.bucket_name,
                CopySource=f"{self.bucket_name}/{o.key}",
                Key=new_key,
            )
            self.client.delete_object(Bucket=self.bucket_name, Key=o.key)

    def get_copy_raw_sql(self, prefix):

        fields = ", ".join(self.task.model.fields_name_list)
        raw_sql = """copy {channel}.{dest_table} ({fields}) from 's3://{bucket_name}/{prefix}' iam_role '{arn}' csv IGNOREHEADER 1 FILLRECORD;""".format(
            channel=get_schema_name(self.task.channel),
            prefix=prefix,
            bucket_name=self.bucket_name,
            arn=REDSHIT_S3_ARN,
            dest_table=self.task.model.model_name,
            fields=fields,
        )

        return raw_sql

    def copy_to_redshift(self):
        # objects = self.get_unprocessed_objects()

        prefix = "application=ingest/channel={channel}/environment={environment}/state={state}/task={task}".format(
            channel=self.task.channel,
            environment=os.environ["RUNNING_ENV"],
            state=UNPROCESSED_STATE,
            task=self.task.params["s3_source_task"],
        )
        objs = list(self.bucket.objects.filter(Prefix=prefix))
        if objs:
            raw_sql = self.get_copy_raw_sql(prefix)
            query = SqlQuery(
                qtype="write_raw_sql",
                raw_sql=raw_sql,
                db_connection=self.task.db_connection,
            )
            query.run()
            # return prefix

    def get_object_key(self) -> str:
        """Build a string representing the S3 path after the bucket_name. It contains the filename.

        Path is in Hive format: /key=value/

        Returns:
            A string: "key=value/key1=value1/..."
        """
        data = self.get_object_key_path_elements()
        result = os.path.join(*["{}={}/".format(d[0], d[1]) for d in data])
        result += "--".join([d[1] for d in data])
        result += ".csv"

        return result

    def get_data_in_file_like_obj_format(self, data):
        fileObj = BytesIO()
        fileObj.write(str.encode(data))

        # Put the cursor at the beginning of the file like obj. Without that, the write file will be empty. # noqa : E501
        fileObj.seek(0)
        return fileObj

    def upload_from_file_object(self, source_filename="staging.csv"):
        """Upload a file to an S3 bucket

        :param data: Data to upload
        :return: True if file was uploaded, else False
        """
        source_file_path = os.path.join(FILE_DIRECTORY, source_filename)

        if os.path.exists(source_file_path):
            object_key = self.get_object_key()

            try:
                self.client.upload_file(
                    source_file_path, self.bucket_name, object_key
                )
                os.remove(source_file_path)
            except ClientError as e:
                raise ClientError(e)

        return True

    def upload_from_file_like_object(self, data):
        """Upload a file to an S3 bucket

        :param data: Data to upload
        :return: True if file was uploaded, else False
        """

        def get_data_in_file_like_obj_format(data):
            fileObj = BytesIO()
            fileObj.write(str.encode(data))

            # Put the cursor at the beginning of the file like obj. Without that, the write file will be empty. # noqa : E501
            fileObj.seek(0)
            return fileObj

        file_obj = get_data_in_file_like_obj_format(data)

        object_key = self.get_object_key()

        try:
            self.client.upload_fileobj(file_obj, self.bucket_name, object_key)
            # self.tmp_objects_list.append(ObjectWrapper(self.bucket.Object(object_key)))
        except ClientError as e:
            logger.error(e)
            return False
        return True

    def copy_from_tmp_objects_list_to_final_dest(self):
        for obj in self.tmp_objects_list:
            dest_obj = ObjectWrapper(self.bucket.Object(f"{obj.key[:-4]}"))
            dest_obj.copy_from(
                CopySource={"Bucket": self.bucket_name, "Key": obj.key}
            )
            dest_obj.wait_until_exists()

    def rollback(self):
        for o in self.tmp_objects_list:
            o.delete()
        print("All tmp files deleted")

    def success_write(self):
        print("Task succefully runned)")
