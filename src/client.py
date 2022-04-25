import logging
import os
from datetime import datetime
from io import BytesIO
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError

from .constants import ENVS_LIST
from .s3wrappers import BucketWrapper, ObjectWrapper


class Client:
    def __init__(self, env):
        if self.check_env(env):
            self.env = env

    @staticmethod
    def check_env(env):
        if env not in ENVS_LIST:
            raise AttributeError(
                "Argument 'env' has to be: {}\n".format(
                    "\n".join(["- " + i for i in ENVS_LIST])
                )
            )
        else:
            return True


class S3Client(Client):
    def __init__(
        self,
        application=None,
        task=None,
        bucket_name=None,
        env="development",
        date=None,
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

    @property
    def bucket(self):
        if not self._bucket:
            self._bucket = self.resource.Bucket(self.bucket_name)
        return self._bucket

    def get_object_key_path_elements(self) -> List[Tuple]:
        """Build list of tuples allowing to build the path.



        Args:
            date: Optionnal. A datetime object.
            Default to None

        Returns:
            A list of tuples: ((data_name, data_value), ...)
        """
        result = []

        # Add elements
        result.append(("application", self.application.name))
        result.append(("environment", self.env))
        result.append(("task", self.task.name))

        if self.date:
            result.append(("year", f"{self.date:%Y}"))
            result.append(("month", f"{self.date:%m}"))
            result.append(("day", f"{self.date:%d}"))

        return result

    def get_object_key(self) -> str:
        """Build a string representing the S3 path after the bucket_name. It contains the filename.

        Directories are in Hive format: /key=value/

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

    def upload_data(self, data):
        """Upload a file to an S3 bucket

        :param data: Data to upload
        :return: True if file was uploaded, else False
        """
        file_obj = self.get_data_in_file_like_obj_format(data)

        object_key = self.get_object_key()

        try:
            response = self.client.upload_fileobj(
                file_obj, self.bucket_name, object_key
            )
            self.tmp_objects_list.append(ObjectWrapper(self.bucket.Object(object_key)))
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def copy_from_tmp_objects_list_to_final_dest(self):
        for obj in self.tmp_objects_list:
            dest_obj = ObjectWrapper(self.bucket.Object(f"{obj.key[:-4]}"))
            dest_obj.copy_from(CopySource={"Bucket": self.bucket_name, "Key": obj.key})
            dest_obj.wait_until_exists()

    def move_object(self, old_key, new_key):
        """Move an S3 object by copying and deleting after.

        Args:
            old_key: str
                The key of the object to "move"
            new_key:
                The object new key

        Returns:
            True if success, else False
        """
        # Copy object A as object B
        # copy_source = {
        #     "Bucket": "linkedin-ingest-dev-staging",
        #     "Key": old_key,
        # }
        # self.resource.meta.client.copy(
        #     copy_source, "linkedin-ingest-dev-staging", new_key
        # )

        object_key = "doc-example-object"
        obj_wrapper = ObjectWrapper(self.bucket.Object(object_key))
        obj_wrapper.put(__file__)
        print(f"Put file object with key {object_key} in bucket {self.bucket.name}.")

        self.client.copy_object(
            Bucket="linkedin-ingest-dev-staging",
            CopySource=f"{self.bucket_name}/{old_key}",
            Key=new_key,
        )
        # Delete the former object A
        response = self.client.delete_object(
            Bucket="linkedin-ingest-dev-staging", Key=old_key
        )

        print(response)

    def
