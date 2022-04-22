import logging
import os
from datetime import datetime
from io import StringIO
from typing import List, Tuple

import boto3
from botocore.exceptions import ClientError


class S3Client:
    def __init__(
        self, application=None, task=None, bucket_name=None, env="development"
    ):
        self.application = application
        self.task = task
        self.bucket_name = bucket_name
        self.client = boto3.client("s3")

    def get_path_tuple(self, date: datetime = None) -> List[Tuple]:
        """Build list of tuples allowing to build the path.



        Args:
            date: Optionnal. A datetime object.
            Default to None

        Returns:
            A list of tuples: ((data_name, data_value), ...)
        """
        result = []

        # Add elements
        result.append("application", self.application)
        # TODO: Add __repr__ method to Task class
        result.append("task", self.task.name)
        result.append("environment", self.env_name)

        if date:
            result.append(("year", date.year))
            result.append(("month", date.month))
            result.append(("day", date.day))

        return result

    def get_hive_formated_path(self, date: datetime = None) -> str:
        """Build a string representing the S3 path after the bucket_name but before file_prefix

        Args:
            date: Optionnal. A datetime object.
            Default to None

        Returns:
            A string: "key=value/key1=value1/..."
        """
        data = self.get_path_tuple(date)
        result = os.path.join(["{}={}/".format(d[0], d[1]) for d in data])
        return result

    @property
    def task_full_path(self, date: datetime = None):
        """# noqa : E501
        Build the path to store result files.

        At the moment, we do not use file prefix. Files will be auto prefixed with numbers by aws.

        Args:
            date: Optionnal. A datetime object.

        Returns:
            A string: "key=value/key1=value1/..."

        """
        if not self._task_full_path:
            # if self.path_date is None:
            #     raise ValueError(
            #         "Type 'datetime.datetime' object required for 'path_datedate'"
            #         f" attribute. Got {self.path_date} of type"
            #         f" {type(self.path_date)} instead."
            #     )

            self._task_full_path = "{}/{}/".format(
                self.bucket_name, self.get_hive_formated_path(date)
            )

        return self._task_full_path

    def get_data_in_file_like_obj_format(self, data):
        fileObj = StringIO.StringIO()
        fileObj.write(data)
        return fileObj

    def upload_data(self, data, bucket, object_name=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        # if object_name is None:
        #     object_name = os.path.basename(file_name)

        # Upload the file
        try:
            response = self.client.upload_fileobj("test", bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
