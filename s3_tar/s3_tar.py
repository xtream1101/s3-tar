import io
import boto3
import logging
import tarfile
from .utils import _create_s3_client, _convert_to_bytes, MIN_S3_SIZE

logger = logging.getLogger(__name__)


class S3Tar:

    def __init__(self, source_bucket, target_key,
                 target_bucket=None, min_file_size=None,
                 session=boto3.session.Session()):
        self.source_bucket = source_bucket
        self.target_bucket = target_bucket
        if self.target_bucket is None:
            self.target_bucket = self.source_bucket

        self.target_key = target_key
        if min_file_size is not None:
            self.min_file_size = _convert_to_bytes(min_file_size)
        else:
            self.min_file_size = None

        if self.target_key.endswith('.tar'):
            self.content_type = 'application/x-tar'
            self.compression_type = None

        elif self.target_key.endswith('.tar.gz'):
            self.content_type = 'application/gzip'
            self.compression_type = 'gz'

        elif self.target_key.endswith('.tar.bz2'):
            self.content_type = 'application/x-bzip2'
            self.compression_type = 'bz2'

        else:
            raise ValueError("Invalid file extension: {}"
                             .format(self.target_key))

        self.all_keys = []
        self.s3 = _create_s3_client(session)

    def tar(self):
        mode = 'w'
        if self.compression_type is not None:
            mode += '|' + self.compression_type
        file_number = 0
        # Keep creating new tar(.gz) as long as there are files left
        while self.all_keys != []:
            file_number += 1

            result_filepath = self.target_key
            if self.min_file_size is not None:
                # Need to number since the number of files is unknown
                compression_ext = ''
                if self.compression_type is not None:
                    compression_ext = '.' + self.compression_type

                result_filepath = '{}-{}.tar{}'.format(
                    self.key.rsplit('.tar', 1)[0],
                    file_number,
                    compression_ext,
                )

            logger.info("Creating file {}".format(result_filepath))
            # Start multipart upload
            resp = self.s3.create_multipart_upload(
                Bucket=self.target_bucket,
                Key=result_filepath,
            )

            parts_mapping = []
            part_num = 0
            current_file_size = 0
            current_part_io = io.BytesIO()
            # If out of files or min size is met, then complete file
            while (self.all_keys != []
                    and (self.min_file_size is None
                         or current_file_size < self.min_file_size)):
                part_num += 1
                logger.debug("Creating part {} of {}"
                             .format(part_num, result_filepath))
                current_part_size = 0
                while (self.all_keys != []
                        and current_part_size < MIN_S3_SIZE * 2):
                    key = self.all_keys.pop()
                    logger.debug("Adding file {} to {}"
                                 .format(key, result_filepath))
                    source_key_io = io.BytesIO()
                    self.s3.download_fileobj(
                        self.source_bucket, key, source_key_io
                    )

                    source_tar_io = io.BytesIO()
                    tar = tarfile.open(fileobj=source_tar_io, mode=mode)
                    info = tarfile.TarInfo(name=key.split('/')[-1])
                    info.size = source_key_io.tell()
                    source_key_io.seek(0)
                    tar.addfile(tarinfo=info, fileobj=source_key_io)

                    if len(self.all_keys) == 0:
                        logger.debug("Create tar's EOF for {}"
                                     .format(result_filepath))
                        # Create the EOF of the tar file
                        tar.close()
                    elif self.compression_type is not None:
                        # When using compression, the data is
                        # not writted to the fileobj until its done
                        tar.fileobj.close()

                    source_tar_io.seek(0)
                    current_part_io.write(source_tar_io.read())
                    current_part_size = current_part_io.tell()

                current_file_size += current_part_size

                logger.debug("Uploading part {} of {}"
                             .format(part_num, result_filepath))
                current_part_io.seek(0)
                part_resp = self.s3.upload_part(
                    Bucket=self.target_bucket,
                    Key=result_filepath,
                    PartNumber=part_num,
                    UploadId=resp['UploadId'],
                    Body=current_part_io.read(),
                )

                parts_mapping.append({
                    'ETag': part_resp['ETag'][1:-1],
                    'PartNumber': part_num,
                })

            # Finish
            self.s3.complete_multipart_upload(
                Bucket=self.target_bucket,
                Key=result_filepath,
                UploadId=resp['UploadId'],
                MultipartUpload={'Parts': parts_mapping},
            )

    def add_files(self, prefix):

        def resp_to_filelist(resp):
            return [(x['Key']) for x in resp['Contents']]

        objects_list = []
        logger.info("Gathering files from folder {}".format(prefix))
        resp = self.s3.list_objects(Bucket=self.source_bucket, Prefix=prefix)
        objects_list.extend(resp_to_filelist(resp))
        logger.debug("Found {} objects so far...".format(len(objects_list)))
        while resp['IsTruncated']:
            last_key = objects_list[-1]
            resp = self.s3.list_objects(
                Bucket=self.source_bucket,
                Prefix=prefix,
                Marker=last_key,
            )
            objects_list.extend(resp_to_filelist(resp))
            logger.debug("Found {} objects so far...".format(len(objects_list)))

        logger.info("Found {} objects in the folder '{}'"
                    .format(len(objects_list),
                            prefix))
        self.all_keys.extend(objects_list)

    def add_file(self, key):
        # TODO make sure key exists, if not log error and do not break
        self.all_keys.append(key)
