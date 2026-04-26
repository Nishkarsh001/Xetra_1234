"""TestS3BucketConnectorMethods"""
import os
import unittest
import boto3

from moto import mock_aws
from Xetra.common.S3 import S3BucketConnector

class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    testing the s3bucket class
    """
    def setUp(self):
        """
        seeting up the mock S3 environment and creating a test bucket
        """
        # mocking s3 connection start
        self.mock_s3 = mock_aws()
        self.mock_s3.start()
        # defining class arguments
        self.s3_access_key ='AWS_ACCESS_KEY_ID'
        self.s3_access_key = 'AWS_SECRET_ACCESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test-bucket'
        # Creating s3 access key for eniviroment variable
        os.environ[self.s3_access_key]='KEY1'
        os.environ[self.s3_access_key]='KEY2'
        #creating a bucket on tge mocked s3 connection
        self.s3=boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,create_bucket_configuration={'LocationConstraint': 'eu-central-1'}) 
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # creating an testing instance
        self.s3_bucket_connector = S3BucketConnector(access_key=self.s3_access_key, secret_key=self.s3_access_key, endpoint_url=self.s3_endpoint_url, bucket=self.s3_bucket_name)



    def tearDown(self):
        """
        tearing down the mock S3 environment
        """
        # mocking s3 connection start
        self.mock_s3.stop()
        
    def test_list_files_in_prefix_ok(self):
        """testing the list_files_in_prefix method of S3BucketConnector
        """ 
        # Expected results
        prefix_exp='prefix/'
        key1_exp=f'{prefix_exp}test1.csv'
        key2_exp=f'{prefix_exp}test2.csv'

        # Test init
        csv_content= """ col1,col2
        val1A,val2A"""
        self.s3_bucket.put_object(Key=key1_exp, Body=csv_content)
        self.s3_bucket.put_object(Key=key1_exp, Body=csv_content)
        # Method Execution
        list_result=self.s3_bucket_connector.list_files_in_prefix(prefix=prefix_exp)
       
        #test after method execution
        self.assertEqual(len(list_result),2 )
        self.assertIn(key1_exp,list_result)
        self.assertIn(key2_exp,list_result)
        # Cleanup after test
        self.s3_bucket.delete_objects(Delete={'Objects': [{'Key': key1_exp}, {'Key': key2_exp}]})
    # print('Oktest')
        
    def test_list_files_in_prefix_wrong_files(self):
        """testing the list_files_in_prefix method of S3BucketConnector when there are no files in the prefix
        """ 
        # Expected results
        prefix_exp='no-prefix/'
       
        # Method Execution
        list_result=self.s3_bucket_connector.list_files_in_prefix(prefix=prefix_exp)
       
        #test after method execution
        
        self.assertTrue(not list_result)
       
    # print('failed_test')
    class TestS3BucketConnectorMethods(unittest.TestCase):

        def setUp(self):
            pass

        def tearDown(self):
            pass

        def test_list_files_in_prefix_ok(self):
            pass

        def test_list_files_in_prefix_wrong_files(self):
            pass


    if __name__ == '__main__':
        unittest.main()