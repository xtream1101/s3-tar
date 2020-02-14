import pytest
from s3_tar.cli import create_parser


def test_parser_no_args():
    parser = create_parser()
    with pytest.raises(SystemExit):
        parser.parse_args('')


def test_parser_check_all_inputs():
    parser = create_parser()
    args = parser.parse_args([
        '--source-bucket', 'my-bucket',
        '--target-bucket', 'other-bucket',
        '--folder', 'mydata/is_here',
        '--filename', 'same_me.tar.gz',
        '--save-metadata',
        '--min-filesize', '2MB',
        '--cache-size', '7',
    ])
    assert args.source_bucket == 'my-bucket'
    assert args.target_bucket == 'other-bucket'
    assert args.folder == 'mydata/is_here'
    assert args.filename == 'same_me.tar.gz'
    assert args.save_metadata is True
    assert args.min_filesize == '2MB'
    assert args.cache_size == 7
