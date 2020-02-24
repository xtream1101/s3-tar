import os
import logging
import argparse
from . import S3Tar

logging.basicConfig(
    level=logging.WARNING,
    format='%(message)s',
)

log_level = os.getenv('S3_TAR_LOG_LEVEL', 'INFO').upper()
logging.getLogger('s3_tar.s3_tar').setLevel(log_level)
logging.getLogger('s3_tar.s3_mpu').setLevel(log_level)


def create_parser():
    parser = argparse.ArgumentParser(
        description='Tar (and compress) files in s3'
    )
    parser.add_argument(
        "--source-bucket",
        help="base bucket to use",
        required=True,
    )
    parser.add_argument(
        "--target-bucket",
        help=("Bucket that the tar will be saved to."
              " Only needed if different then source bucket"),
        default=None,
    )
    parser.add_argument(
        "--folder",
        help="folder whose contents should be combined",
        required=True,
    )
    parser.add_argument(
        "--filename",
        help=("Output filename for the tar file."
              "\nExtension: tar, tar.gz, or tar.bz2"),
        required=True,
    )
    parser.add_argument(
        "--save-metadata",
        help="If a file has metadata, save it to a .metadata.json file",
        action='store_true',
    )
    parser.add_argument(
        "--remove",
        help="Delete files that were added to the tar file",
        action='store_true',
    )
    parser.add_argument(
        "--preserve-paths",
        help="Preserve the path layout relative to the input folder",
        action='store_true',
    )
    parser.add_argument(
        "--allow-dups",
        help="Allow duplicate filenames to be saved into the tar file",
        action='store_true',
    )
    parser.add_argument(
        "--min-filesize",
        help=("Use to create multiple files if needed."
              " Min filesize of the tar'd files"
              " in [B,KB,MB,GB,TB]. e.x. 5.2GB"),
        default=None,
    )
    parser.add_argument(
        "--cache-size",
        help="Number of files to download into memory at a time",
        type=int,
        default=5,
    )

    return parser


def cli():
    # No need to run testson these. They are tested separately
    args = create_parser().parse_args()  # pragma: no cover
    job = S3Tar(
        args.source_bucket,
        args.filename,
        target_bucket=args.target_bucket,
        cache_size=args.cache_size,
        min_file_size=args.min_filesize,
        remove_keys=args.remove,
        save_metadata=args.save_metadata,
        allow_dups=args.allow_dups,
    )  # pragma: no cover
    job.add_files(
        args.folder,
        preserve_paths=args.preserve_paths,
    )  # pragma: no cover
    job.tar()  # pragma: no cover
