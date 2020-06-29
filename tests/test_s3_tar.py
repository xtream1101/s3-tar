import io
import time
import boto3
import pytest
from s3_tar import S3Tar
from moto import mock_s3


###
# add_file
###
def test_add_file():
    tar = S3Tar('my-bucket', 'my-data.tar')
    tar.add_file('this_is_a/test.file')
    tar.add_file('this_is_a/test.file')  # make sure it de-dups for same key
    tar.add_file('this_is_another/test.file', folder='in/here')

    assert tar.all_keys == {('test.file', 'this_is_a/test.file'),
                            ('in/here/test.file', 'this_is_another/test.file')}


def test_add_file_dup_member_name_fail():
    tar = S3Tar('my-bucket', 'my-data.tar')
    tar.add_file('this_is_a/test.file')
    tar.add_file('this_is_a/test.file')  # Fine sine the key is a dup

    # Test the logic on how keys are checked, this should be fine
    tar.add_file('person.txt')
    tar.add_file('son.txt')

    # Raise exception since the member name is a dup with diff key
    with pytest.raises(ValueError):
        tar.add_file('this_is_another/test.file')


def test_add_file_dup_member_name_allow():
    tar = S3Tar('my-bucket', 'my-data.tar', allow_dups=True)
    tar.add_file('this_is_a/test.file')
    tar.add_file('this_is_a/test.file')  # Fine sine the key is a dup
    tar.add_file('this_is_another/test.file')

    assert tar.all_keys == {('test.file', 'this_is_a/test.file'),
                            ('test.file', 'this_is_another/test.file')}


###
# add_files
###
@mock_s3
def test_add_files_dedup_allow():
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
    s3.put_object(
        Bucket='my-bucket',
        Key='different_folder/thing2.txt',
        Body=b'Test File Contents',
    )

    tar = S3Tar('my-bucket', 'my-data.tar',
                allow_dups=True, session=session)
    tar.add_files('some_folder')
    tar.add_files('some_folder')  # make sure it de-dups
    tar.add_files('different_folder')

    assert tar.all_keys == {('thing1.txt', 'some_folder/thing1.txt'),
                            ('thing2.txt', 'some_folder/thing2.txt'),
                            ('thing2.txt', 'different_folder/thing2.txt')}


@mock_s3
def test_add_files_dedup_fail():
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
    s3.put_object(
        Bucket='my-bucket',
        Key='different_folder/thing2.txt',
        Body=b'Test File Contents',
    )

    tar = S3Tar('my-bucket', 'my-data.tar',
                allow_dups=False, session=session)
    tar.add_files('some_folder')
    tar.add_files('some_folder')  # make sure it de-dups
    with pytest.raises(ValueError):
        tar.add_files('different_folder')


@mock_s3
def test_add_files_folders():
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
        Key='some_folder/foo/thing2.txt',
        Body=b'Test File Contents',
    )

    tar = S3Tar('my-bucket', 'my-data.tar', session=session)
    tar.add_files('some_folder/', folder='my_data', preserve_paths=True)

    output = {
        ('my_data/thing1.txt', 'some_folder/thing1.txt'),
        ('my_data/foo/thing2.txt', 'some_folder/foo/thing2.txt'),
    }
    assert tar.all_keys == output


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
def test_download_source_file_contents():
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
# _add_key_to_cache
###
@mock_s3
def test_add_key_to_cache_with_metadata():
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
    tar = S3Tar('my-bucket', 'my-data.tar',
                save_metadata=True, session=session)
    tar._add_key_to_cache('thing1.txt', 'thing1.txt')

    assert len(tar.file_cache) == 2


@mock_s3
def test_add_key_to_cache_no_metadata():
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
    tar = S3Tar('my-bucket', 'my-data.tar',
                save_metadata=False, session=session)
    tar._add_key_to_cache('thing1.txt', 'thing1.txt')

    assert len(tar.file_cache) == 1


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
    tar_io = tar._get_tar_source_metadata(
        'my_things/data/thing1.txt', 'thing1.txt'
    )
    # Test that the fn is not seeking to 0
    assert tar_io.tell() != 0

    tar_io.seek(0)
    tar_obj = tarfile.open(fileobj=tar_io, mode='r')
    assert tar_obj.getmember('my_things/data/thing1.txt.metadata.json')


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
    tar_io = tar._get_tar_source_metadata('thing1.txt', 'thing1.txt')
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
    tar_io = tar._get_tar_source_data('mydata/thing1.txt', 'thing1.txt')
    # Test that the fn is not seeking to 0
    assert tar_io.tell() != 0

    tar_io.seek(0)
    # Test tar file has content
    tar_obj = tarfile.open(fileobj=tar_io, mode='r')
    assert tar_obj.getmember('mydata/thing1.txt')

    # Test that timestamp was preserved
    assert tar_obj.getmember('mydata/thing1.txt').get_info()['mtime'] != 0


###
# _save_bytes_to_tar
###
def test_bytes_to_tar_output_tar():
    tar = S3Tar('my-bucket', 'my-data.tar')
    source_io = io.BytesIO()
    source_io.write(b'Beep boop')
    output = tar._save_bytes_to_tar('test.tar', source_io, time.time(), 'w')
    assert output.tell() == 2048


def test_bytes_to_tar_output_tar_compression():
    tar = S3Tar('my-bucket', 'my-data.tar')
    source_io = io.BytesIO()
    source_io.write(b'Beep boop')
    output = tar._save_bytes_to_tar('test.tar.gz', source_io, time.time(), 'w|gz')
    assert output.tell() == 156


def test_bytes_to_tar_save_timestamp_tar():
    import time
    import tarfile

    tar = S3Tar('my-bucket', 'my-data.tar')
    source_io = io.BytesIO()
    source_io.write(b'Beep boop')
    source_mtime = time.time()
    output = tar._save_bytes_to_tar('foobar.txt', source_io, source_mtime, 'w')
    output.seek(0)

    tar_test_file = tarfile.open(fileobj=output)
    member = tar_test_file.getmembers()[0]
    assert member.get_info()['mtime'] != 0


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
