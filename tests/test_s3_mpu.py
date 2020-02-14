import io
import boto3
import pytest
from moto import mock_s3
from s3_tar.s3_mpu import S3MPU
import botocore.exceptions


@mock_s3()
def test_s3_multipart_upload_success():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-archive')

    test_source_io = io.BytesIO()
    test_source_io.write(b'hello World!')

    mpu = S3MPU(s3, 'my-archive', 'archive.txt')

    assert mpu.upload_part(test_source_io) is True
    assert mpu.complete() is True


@mock_s3()
def test_s3_multipart_upload_part_fail_no_seek():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-archive')

    test_source_io = io.BytesIO()
    test_source_io.write(b'hello World!')

    mpu = S3MPU(s3, 'my-archive', 'archive.txt')

    with pytest.raises(AttributeError):
        mpu.upload_part('foo')


@mock_s3()
def test_s3_multipart_upload_fail_complete():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-archive')

    test_source_io = io.BytesIO()
    test_source_io.write(b'hello World!')

    mpu = S3MPU(s3, 'my-archive', 'archive.txt')

    mpu.upload_part(io.BytesIO())
    mpu.upload_part(io.BytesIO())
    with pytest.raises(botocore.exceptions.ClientError):
        # Will cause the s3 upload to cause `EntityTooSmall`
        # since the first part is <5MB
        mpu.complete()
