import os
import logging
import argparse
from . import S3Tar

logging.basicConfig(
    level=os.getenv('S3_TAR_LOG_LEVEL', 'INFO').upper(),
    format='%(message)s',
)


def cli():
    parser = argparse.ArgumentParser(
        description='Tar (and compress) files in s3'
    )
    parser.add_argument(
        "--bucket",
        help="base bucket to use",
        required=True,
    )
    parser.add_argument(
        "--folder",
        help="folder whose contents should be combined",
        required=True,
    )
    parser.add_argument(
        "--filename",
        help="Output filename for the tar file.\nExtension: tar, or tar.gz",
        required=True,
    )
    parser.add_argument(
        "--min-filesize",
        help=("Use to create multiple files if needed."
              " Min filesize of the tar'd files"
              " in [B,KB,MB,GB,TB]. e.x. 5.2GB"),
        default=None,
    )
    args = parser.parse_args()

    job = S3Tar(args.bucket, args.filename, min_file_size=args.filesize)
    job.add_files(args.folder)
    job.tar()
