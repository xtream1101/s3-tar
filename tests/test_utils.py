import pytest
from s3_tar.utils import _convert_to_bytes


def test_convert_returns_float():
    assert isinstance(_convert_to_bytes('100B'), float)


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
