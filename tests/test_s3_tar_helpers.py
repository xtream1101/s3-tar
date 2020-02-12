from s3_tar import S3Tar
from moto import mock_s3


###
# _get_tar_file_path
###
def test_get_tar_file_path_no_min_tar():
    tar = S3Tar('my-bucket', 'my-data.tar')
    assert tar._get_tar_file_path(1) == 'my-data.tar'


def test_get_tar_file_path_with_min_tar():
    tar = S3Tar('my-bucket', 'my-data.tar', min_file_size='10MB')
    assert tar._get_tar_file_path(1) == 'my-data-1.tar'


def test_get_tar_file_path_with_min_tar_compressed():
    tar = S3Tar('my-bucket', 'my-data.tar.gz', min_file_size='10MB')
    assert tar._get_tar_file_path(2) == 'my-data-2.tar.gz'


###
# _is_complete
###
def test_is_complete_empty():
    tar = S3Tar('my-bucket', 'my-data.tar')
    tar.all_keys = []
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


