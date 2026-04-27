"""Connector and methods accessing S3"""
from importlib.resources import files
from Xetra.common.constants import S3FileTypes
from io import StringIO
from io import BytesIO
import os
import logging
from Xetra.common.custom_exceptions import WrongFormatException
import boto3
import pandas as pd

class S3BucketConnector():
    """
    Class for interacting with S3 Buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector

        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                      aws_secret_access_key=os.environ[secret_key]
        )

        if endpoint_url:
            self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        else:
            self._s3 = self.session.resource(service_name='s3')
        self._bucket = self._s3.Bucket(bucket) # type: ignore

    def list_files_in_prefix(self,prefix: str):
        """
        List all files in a given prefix within the S3 bucket.

        :param prefix: The prefix to filter files by
        :return: A list of file keys
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str, encoding: str = 'utf-8', sep: str = ','):
        """
        reading a csv file from the S3 bucket and returning a dataframe

        :param key: key of the file that should be read
        :encoding: encoding of the data inside the csv file
        :sep: seperator of the csv file

        returns:
            data_frame: Pandas DataFrame containing the data of the csv file
        """
        self._logger.info(
            'Reading file %s/%s/%s',
            self.endpoint_url,
            self._bucket.name,
            key
        )
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)

        return data_frame

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str):
        """
        writing a Pandas DataFrame to S3
        supported formats: .csv, .parquet

        :data_frame: Pandas DataFrame that should be written
        :key: target key of the saved file
        :file_format: format of the saved file
        """

        if data_frame.empty:
            self._logger.info(
                'The dataframe is empty! No file will be written!'
            )
            return None

        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self._put_object(out_buffer, key)

        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self._put_object(out_buffer, key)

        self._logger.info(
            'The file format %s is not supported to be written to s3!',
            file_format
        )
        raise WrongFormatException()

    def _put_object(self, out_buffer: StringIO | BytesIO, key: str):
        """
        Helper function for self.write_df_to_s3()

        :out_buffer: StringIO | BytesIO that should be written
        :key: target key of the saved file
        """

        self._logger.info(
            'Writing file to %s/%s/%s',
            self.endpoint_url,
            self._bucket.name,
            key
        )

        self._bucket.put_object(
            Body=out_buffer.getvalue(),
            Key=key
        )
        return True
