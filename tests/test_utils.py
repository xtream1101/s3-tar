import pytest
from s3_tar.utils import _convert_to_bytes, _threads, MIN_S3_SIZE


###
# Consts
###
def test_min_s3_size():
    assert MIN_S3_SIZE == 5242880


###
# _convert_to_bytes
###
def test_convert_returns_float():
    assert isinstance(_convert_to_bytes('100B'), float)


def test_convert_none():
    assert _convert_to_bytes(None) is None


def test_convert_b_using_different_inputs():
    assert _convert_to_bytes('100B') == 100
    assert _convert_to_bytes('100b') == 100
    assert _convert_to_bytes('100 B') == 100
    assert _convert_to_bytes('100.3') == 100.3
    assert _convert_to_bytes(100.0) == 100


def test_convert_kb():
    assert _convert_to_bytes('1KB') == 1024
    assert _convert_to_bytes('1kb') == 1024


def test_convert_mb():
    assert _convert_to_bytes('1MB') == 1048576


def test_convert_gb():
    assert _convert_to_bytes('1GB') == 1073741824


def test_convert_tb():
    assert _convert_to_bytes('1TB') == 1099511627776


def test_convert_bad_input():
    with pytest.raises(ValueError):
        _convert_to_bytes('1xy')


###
# _threads
###
def test_threads_success():
    def _callback(num, static_arg, static_num=None):
        return num + static_arg + static_num

    output = _threads(1, [1, 2, 3], _callback, 1, static_num=3)
    assert output == [5, 6, 7]


def test_threads_failing():
    def _callback(num, static_arg, static_num=None):
        return num + static_arg + static_num

    output = _threads(1, [1, '2', 3], _callback, 1, static_num=3)
    assert output == [5, None, 7]
