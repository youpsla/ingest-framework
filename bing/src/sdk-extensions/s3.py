import io
import os
from datetime import datetime

import boto3
import requests
from bingads.exceptions import FileDownloadException
from bingads.v13.reporting import ReportingDownloadOperation
from bingads.v13.reporting.reporting_operation import TlsHttpAdapter

from ..bingads_client import OUTPUT_TYPE, S3_BUCKET, S3_DIRECTORY


class S3Storage:
    def __init_(self, s3bucket):



class CustomReportingDownloadOperation(ReportingDownloadOperation):
    def save(self, raw, result_file_name):
        file = io.BytesIO(bytes(raw, encoding="utf-8"))
        s3 = boto3.resource("s3")
        bucket = s3.Bucket(S3_BUCKET)
        bucket_object = bucket.Object(os.path.join(S3_DIRECTORY, result_file_name))
        bucket_object.upload_fileobj(file)

    def download_result_file(
        self,
        result_file_directory,
        result_file_name,
        timeout_in_milliseconds=None,
    ):
        """Download file with specified URL and download parameters.

        :param result_file_directory: The download result local directory name.
        :type result_file_directory: str
        :param result_file_name: The download result local file name.
        :type result_file_name: str | None
        :param decompress: Determines whether to decompress the ZIP file.
                            If set to true, the file will be decompressed after download.
                            The default value is false, in which case the downloaded file is not decompressed.
        :type decompress: bool
        :param overwrite: Indicates whether the result file should overwrite the existing file if any.
        :type overwrite: bool
        :return: The download file path.
        :rtype: str
        :param timeout_in_milliseconds: (optional) timeout for download result file in milliseconds
        :type timeout_in_milliseconds: int
        """

        if result_file_directory is None:
            raise ValueError("result_file_directory cannot be None.")

        url = self.final_status.report_download_url

        if url is None or url == "":
            return None

        if result_file_name is None:
            raise ValueError("result_file_name cannot be None.")

        # TODO: Test if S3 bucket exist

        headers = {
            "User-Agent": USER_AGENT,
        }
        s = requests.Session()
        s.mount("https://", TlsHttpAdapter())
        timeout_seconds = (
            None
            if timeout_in_milliseconds is None
            else timeout_in_milliseconds / 1000.0
        )
        try:
            r = s.get(
                url, headers=headers, stream=True, verify=True, timeout=timeout_seconds
            )
        except requests.Timeout as ex:
            raise FileDownloadException(ex)
        r.raise_for_status()
        # try:
        #     with open(zip_file_path, "wb") as f:
        #         for chunk in r.iter_content(chunk_size=4096):
        #             if chunk:
        #                 f.write(chunk)
        #                 f.flush()
        #     if decompress:
        #         with contextlib.closing(zipfile.ZipFile(zip_file_path)) as compressed:
        #             first = compressed.namelist()[0]
        #             with open(result_file_path, "wb") as f, compressed.open(
        #                 first, "r"
        #             ) as cc:
        #                 shutil.copyfileobj(cc, f)
        # except Exception as ex:
        #     raise ex
        # finally:
        #     if decompress and os.path.exists(zip_file_path):
        #         os.remove(zip_file_path)
        self.save(r.raw, result_file_name)
