import io
import boto3
from s3_tar import S3Tar
from moto import mock_s3


###
# add_file
###
def test_add_file():
    tar = S3Tar('my-bucket', 'my-data.tar')
    tar.add_file('this_is_a/test.file')
    tar.add_file('this_is_a/test.file')  # make sure it de-dups
    tar.add_file('this_is_another/test.file')

    assert tar.all_keys == {'this_is_a/test.file', 'this_is_another/test.file'}


###
# add_files
###
@mock_s3
def test_add_files_no_dups():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='some_folder/thing1.txt',
        Body=b'Test File Contents',
    )
    s3.put_object(
        Bucket='my-bucket',
        Key='some_folder/thing2.txt',
        Body=b'Test File Contents',
    )

    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    tar.add_files('some_folder')
    tar.add_files('some_folder')  # make sure it de-dups

    assert tar.all_keys == {'some_folder/thing1.txt', 'some_folder/thing2.txt'}


@mock_s3
def test_add_files_missing_dir():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')

    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    tar.add_files('does_not_exist_folder')

    assert tar.all_keys == set()


###
# _download_source_metadata
###
@mock_s3
def test_download_metadata_has():
    import json
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='thing1.txt',
        Body=b'Test File Contents',
        Metadata={'foo': 'bar'},
    )
    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    metadata_io = tar._download_source_metadata('thing1.txt')
    # Test that the fn is not seeking to 0
    assert metadata_io.tell() != 0

    metadata_io.seek(0)
    metadata_json = json.load(metadata_io)

    assert metadata_json == {'foo': 'bar'}


@mock_s3
def test_download_metadata_none():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='thing1.txt',
        Body=b'Test File Contents',
    )
    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    metadata_io = tar._download_source_metadata('thing1.txt')
    assert metadata_io is None


###
# _download_source_file
###
@mock_s3
def test_download_source_file():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='thing1.txt',
        Body=b'Test File Contents',
    )
    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    source_io = tar._download_source_file('thing1.txt')
    # Test that the fn is not seeking to 0
    assert source_io.tell() != 0
    source_io.seek(0)
    assert source_io.read() == b'Test File Contents'


###
# _get_tar_source_metadata
###
@mock_s3
def test_get_tar_source_metadata_has():
    import tarfile
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='thing1.txt',
        Body=b'Test File Contents',
        Metadata={'foo': 'bar'},
    )
    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    tar_io = tar._get_tar_source_metadata('thing1.txt')
    # Test that the fn is not seeking to 0
    assert tar_io.tell() != 0

    tar_io.seek(0)
    tar_obj = tarfile.open(fileobj=tar_io, mode='r')
    assert tar_obj.getmember('thing1.txt.metadata.json')


@mock_s3
def test_get_tar_source_metadata_none():
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='thing1.txt',
        Body=b'Test File Contents',
    )
    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    tar_io = tar._get_tar_source_metadata('thing1.txt')
    assert tar_io is None


###
# _get_tar_source_data
###
@mock_s3
def test_get_tar_source_data():
    import tarfile
    session = boto3.session.Session()
    s3 = session.client('s3')
    # Need to create the bucket since this is in Moto's 'virtual' AWS account
    s3.create_bucket(Bucket='my-bucket')
    s3.put_object(
        Bucket='my-bucket',
        Key='thing1.txt',
        Body=b'Test File Contents',
    )
    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    tar_io = tar._get_tar_source_data('thing1.txt')
    # Test that the fn is not seeking to 0
    assert tar_io.tell() != 0

    tar_io.seek(0)
    tar_obj = tarfile.open(fileobj=tar_io, mode='r')
    assert tar_obj.getmember('thing1.txt')


###
# _save_bytes_to_tar
###
def test_bytes_to_tar_output_tar():
    tar = S3Tar('my-bucket', 'my-data.tar')
    source_io = io.BytesIO()
    source_io.write(b'Beep boop')
    output = tar._save_bytes_to_tar('test.tar', source_io, 'w')
    assert output.tell() == 1024


def test_bytes_to_tar_output_tar_compression():
    tar = S3Tar('my-bucket', 'my-data.tar')
    source_io = io.BytesIO()
    source_io.write(b'Beep boop')
    output = tar._save_bytes_to_tar('test.tar.gz', source_io, 'w|gz')
    assert output.tell() == 91


###
# _add_file_number
###
def test_add_file_number_no_min_tar():
    tar = S3Tar('my-bucket', 'my-data.tar')
    assert tar._add_file_number(1) == 'my-data.tar'


def test_add_file_number_with_min_tar():
    tar = S3Tar('my-bucket', 'my-data.tar', min_file_size='10MB')
    assert tar._add_file_number(1) == 'my-data-1.tar'


def test_add_file_number_with_min_tar_compressed():
    tar = S3Tar('my-bucket', 'my-data.tar.gz', min_file_size='10MB')
    assert tar._add_file_number(2) == 'my-data-2.tar.gz'


###
# _is_complete
###
def test_is_complete_empty():
    tar = S3Tar('my-bucket', 'my-data.tar')
    tar.all_keys = set()
    tar.file_cache = []
    assert tar._is_complete() is True


def test_is_complete_has_keys():
    tar = S3Tar('my-bucket', 'my-data.tar')
    tar.all_keys = ['key1', 'key2']
    assert tar._is_complete() is False


def test_is_complete_has_cache():
    tar = S3Tar('my-bucket', 'my-data.tar')
    tar.file_cache = ['obj1']
    assert tar._is_complete() is False
