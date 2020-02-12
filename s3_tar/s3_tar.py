import io
import json
import time
import boto3
import logging
import tarfile
import threading
from .s3_mpu import S3MPU
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
        """Start the tar'ing process with what has been added
        """
        # Kick off a job to start download sources in the background
        cache_t = threading.Thread(target=self._pre_fetch_files)
        cache_t.daemon = True
        cache_t.start()

        # Keep creating new tar(.gz) as long as there are files left
        file_number = 0
        while self._is_complete() is False:
            file_number += 1
            self._new_file_upload(file_number)

    def _new_file_upload(self, file_number):
        """Start a new multipart upload for the tar file

        Args:
            file_number (int): The number of this file getting created
        """
        result_filepath = self._add_file_number(file_number)

        # Start multipart upload
        mpu = S3MPU(self.s3, self.target_bucket, result_filepath)

        current_file_size = 0
        # If out of files or min size is met, then complete file
        while (self._is_complete() is False
                and (self.min_file_size is None
                     or current_file_size < self.min_file_size)):
            current_part_io = self._get_part_contents()

            current_file_size += current_part_io.tell()
            mpu.upload_part(current_part_io)

        mpu.complete()

    def _get_part_contents(self):
        """Create multipart upload contents
        Pull files from file cache and append until file is large enough
        to be uploaded to s3's multi prt upload

        Returns:
            BytesIO: io.BytesIO object to be upload
        """
        current_io = io.BytesIO()
        current_size = 0
        while current_size < MIN_S3_SIZE * 2:
            source_tar_io = self._get_file_from_cache()
            if source_tar_io is None:
                # Must be the end since no more files to add
                break
            source_tar_io.seek(0)
            current_io.write(source_tar_io.read())
            source_tar_io.close()  # Cleanup
            # New current size
            current_size = current_io.tell()

        return current_io

    def _get_file_from_cache(self):
        """Pull content from the file cache to build a part to get uploaded

        Returns:
            BytesIO: io.BytesIO object
        """
        while self._is_complete() is False:
            if self.file_cache != []:
                # Must pop from idx 0
                # the last item in the file cache has the EOF bytes
                return self.file_cache.pop(0)
            time.sleep(0.1)

        return None

    def _add_file_number(self, file_number):
        """Add file number to tar file if needed

        If its possible that there may need to be multiple tar files,
        number them so they do not get overwritten.
        This would happen if self.min_file_size is set

        Args:
            file_number (int): The number to give the file

        Returns:
            str: The filename to use in s3
        """
        result_filepath = self.target_key
        if self.min_file_size is not None:
            # Need to number since the number of files is unknown
            compression_ext = ''
            if self.compression_type is not None:
                compression_ext = '.' + self.compression_type

            result_filepath = '{}-{}.tar{}'.format(
                result_filepath.rsplit('.tar', 1)[0],
                file_number,
                compression_ext,
            )
        return result_filepath

    def _is_complete(self):
        """Are they any files left to be uploaded/tar'd

        Returns:
            bool: If we can complete this tar'ing process or not
        """
        return self.all_keys == [] and self.file_cache == []

    def _pre_fetch_files(self):
        """Started as a background job to keep adding files to the
        file cache to speed things along
        """
        def _fetch(key):
            while len(self.file_cache) >= self.cache_size:
                # Hold here until more files are needed in the cache
                time.sleep(0.1)

            logger.debug("Adding to cache {}".format(key))
            self._add_key_to_cache(key)

        _threads(self.cache_size, self.all_keys, _fetch)
        self.all_keys = []  # clear now that all have been processed

    def _add_key_to_cache(self, key):
        """Get the source of an s3 key (and its metadata if needed) and add
        it to the file cache

        Args:
            key (str): the key to download form s3
        """
        if self.save_metadata is True:
            metadata_io = self._get_tar_source_metadata(key)
            if metadata_io is not None:
                logger.debug("Adding metadata file to cache {}".format(key))
                self.file_cache.append(metadata_io)

        self.file_cache.append(
            self._get_tar_source_data(key)
        )

    def _get_tar_source_data(self, key):
        """Download source file and generate a tar from it

        Args:
            key (str): File from s3 to download

        Returns:
            io.BytesIO: BytesIO object of the tar file
        """
        source_key_io = self._download_source_file(key)
        source_tar_io = self._save_bytes_to_tar(
            key.split('/')[-1],
            source_key_io,
            mode=self.mode,
        )
        source_key_io.close()  # Cleanup
        return source_tar_io

    def _get_tar_source_metadata(self, key):
        """Get metadata from the s3 file
        If a file has metadata then add it to a tar file
        If not, then return None

        Args:
            key (str): S3 file to get metadata from

        Returns:
            io.BytesIO|None: BytesIO object of the tar file
        """
        source_metadata_io = self._download_source_metadata(key)
        if source_metadata_io is None:
            return None

        source_metadata_tar_io = self._save_bytes_to_tar(
            key.split('/')[-1] + '.metadata.json',
            source_metadata_io,
            mode=self.mode,
        )
        source_metadata_io.close()  # Cleanup
        return source_metadata_tar_io

    def _download_source_file(self, key):
        """Download source file from s3 into a BytesIO object

        Args:
            key (str): S3 file to download

        Returns:
            io.BytesIO: BytesIO object of the contents
        """
        source_key_io = io.BytesIO()
        self.s3.download_fileobj(self.source_bucket, key, source_key_io)
        return source_key_io

    def _download_source_metadata(self, key):
        """Get metadata from an s3 file

        Args:
            key (str): S3 file to pull metadata from

        Returns:
            io.BytesIO|None: BytesIO object of the tar file
        """
        source_metadata_io = io.BytesIO()
        metadata = self.s3.head_object(
            Bucket=self.source_bucket,
            Key=key,
        )['Metadata']
        if metadata == {}:
            return None

        source_metadata_io.write(json.dumps(metadata).encode('utf-8'))
        return source_metadata_io

    @classmethod
    def _save_bytes_to_tar(cls, name, source, mode):
        """Convert raw bytes into a tar

        Args:
            name (str): Filename inside the tar
            source (io.BytesIO): The data to be saved into the tar
            mode (str): The file mode in which to open the tar file

        Returns:
            io.BytesIO: BytesIO object of the tar'd data
        """
        source_tar_io = io.BytesIO()
        tar = tarfile.open(fileobj=source_tar_io, mode=mode)
        info = tarfile.TarInfo(name=name)
        info.size = source.tell()
        source.seek(0)
        tar.addfile(tarinfo=info, fileobj=source)

        if '|' in mode:
            # When using compression, the data is
            # not writted to the fileobj until its done
            tar.fileobj.close()

        return source_tar_io

    def add_files(self, prefix):
        """Add s3 files from a directory inside the source bucket

        self.all_keys gets added to directly

        Args:
            prefix (str): Folder path inside the source bucket
        """
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
        """Add a single file at a time to be tar'd

        self.all_keys gets added to directly

        Args:
            key (str): The full path to a single s3 file in the source bucket
        """
        # TODO make sure key exists, if not log error and do not break
        self.all_keys.append(key)
