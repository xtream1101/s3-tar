import io
import json
import time
import boto3
import logging
import tarfile
import threading
from .utils import _create_s3_client, _convert_to_bytes, _threads, MIN_S3_SIZE

logger = logging.getLogger(__name__)


class S3Tar:

    def __init__(self, source_bucket, target_key,
                 target_bucket=None,
                 min_file_size=None,
                 cache_size=5,
                 save_metadata=False,
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

        self.save_metadata = save_metadata

        self.mode = 'w'
        if self.compression_type is not None:
            self.mode += '|' + self.compression_type

        self.all_keys = []  # Keys the user adds
        self.file_cache = []  # io objects that are ready to be combined
        self.cache_size = cache_size
        if self.cache_size is None or self.cache_size <= 0:
            raise ValueError("cache size must be 1 or larger")

        self.s3 = _create_s3_client(session)

    def tar(self):
        # Start fetching files in the background
        cache_t = threading.Thread(target=self._pre_fetch_files)
        cache_t.daemon = True
        cache_t.start()

        file_number = 0
        # Keep creating new tar(.gz) as long as there are files left
        while self._is_complete() is False:
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
            # If out of files or min size is met, then complete file
            while (self._is_complete() is False
                    and (self.min_file_size is None
                         or current_file_size < self.min_file_size)):
                current_part_io = io.BytesIO()
                part_num += 1
                logger.debug("Creating part {} of {}"
                             .format(part_num, result_filepath))
                current_part_size = 0
                while (self._is_complete() is False
                        and current_part_size < MIN_S3_SIZE * 2):
                    source_tar_io = self._get_file_from_cache()
                    source_tar_io.seek(0)
                    current_part_io.write(source_tar_io.read())
                    source_tar_io.close()  # Cleanup
                    # New current size
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
                current_part_io.close()  # Cleanup

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

    def _is_complete(self):
        if self.all_keys == [] and self.file_cache == []:
            return True
        return False

    def _pre_fetch_files(self):
        # Make sure this is the last file the tar saves, will have EOF bytes
        last_file_key = self.all_keys.pop()

        def _fetch(key):
            logger.debug("Adding to cache {}".format(key))
            self._add_key_to_cache(self.source_bucket, key)
            if self.save_metadata is True:
                self._add_metadata_to_cache(self.source_bucket, key)

            while len(self.file_cache) >= self.cache_size:
                # Hold here until more files are needed
                time.sleep(0.1)

        _threads(self.cache_size, self.all_keys, _fetch)
        # Now add last file (and metadata if needed)
        if self.save_metadata is True:
            self._add_metadata_to_cache(self.source_bucket, last_file_key)
        logger.debug("Adding last file to cache {}".format(last_file_key))
        self._add_key_to_cache(
            self.source_bucket, last_file_key, is_last_file=False
        )
        self.all_keys = []  # clear now that all have been processed

    def _add_key_to_cache(self, bucket, key, is_last_file=False):
        self.file_cache.append(
            self._get_source_data(bucket, key, is_last_file=is_last_file)
        )

    def _add_metadata_to_cache(self, bucket, key):
        metadata_io = self._get_source_metadata(bucket, key)
        if metadata_io is not None:
            logger.debug("Adding metadata file to cache {}".format(key))
            self.file_cache.append(metadata_io)

    def _get_source_data(self, bucket, key, is_last_file=False):
        source_key_io = self._download_source_file(
            bucket, key
        )
        source_tar_io = self._save_bytes_to_tar(
            key.split('/')[-1],
            source_key_io,
            mode=self.mode,
            close=is_last_file,
        )
        source_key_io.close()  # Cleanup
        return source_tar_io

    def _get_source_metadata(self, bucket, key):
        source_metadata_io = self._download_source_metadata(bucket, key)
        if source_metadata_io is None:
            return None

        source_metadata_tar_io = self._save_bytes_to_tar(
            key.split('/')[-1] + '.metadata.json',
            source_metadata_io,
            mode=self.mode,
        )
        source_metadata_io.close()  # Cleanup
        return source_metadata_tar_io

    def _get_file_from_cache(self):
        while True:
            if self.file_cache != []:
                # Must pop from idx 0
                # the last item in the file cache has the EOF bytes
                return self.file_cache.pop(0)
            time.sleep(0.1)

    def _download_source_file(self, bucket, key):
        source_key_io = io.BytesIO()
        self.s3.download_fileobj(bucket, key, source_key_io)
        return source_key_io

    def _download_source_metadata(self, bucket, key):
        source_metadata_io = io.BytesIO()
        metadata = self.s3.head_object(Bucket=bucket, Key=key)['Metadata']
        if metadata == {}:
            return None

        source_metadata_io.write(json.dumps(metadata).encode('utf-8'))
        return source_metadata_io

    @classmethod
    def _save_bytes_to_tar(cls, name, source, mode, close=False):
        source_tar_io = io.BytesIO()
        tar = tarfile.open(fileobj=source_tar_io, mode=mode)
        info = tarfile.TarInfo(name=name)
        info.size = source.tell()
        source.seek(0)
        tar.addfile(tarinfo=info, fileobj=source)

        if close is True:
            # Create the EOF of the tar file
            tar.close()
        elif '|' in mode:
            # When using compression, the data is
            # not writted to the fileobj until its done
            tar.fileobj.close()

        return source_tar_io

    def add_files(self, prefix):

        def resp_to_filelist(resp):
            return [(x['Key']) for x in resp['Contents']]

        objects_list = []
        logger.info("Gathering files from folder {}".format(prefix))
        resp = self.s3.list_objects_v2(Bucket=self.source_bucket, Prefix=prefix)
        objects_list.extend(resp_to_filelist(resp))
        objects_list.remove(prefix)  # Do not add the prefix as a file
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
