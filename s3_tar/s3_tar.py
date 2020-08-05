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
                 remove_keys=False,
                 allow_dups=False,
                 s3_max_retries=4,
                 part_size_multiplier=None,
                 session=boto3.session.Session()):
        self.allow_dups = allow_dups
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

        if part_size_multiplier is None:
            self.part_size_multiplier = 10
        else:
            if (not isinstance(part_size_multiplier, int)
                    or int(part_size_multiplier) <= 0):
                logger.warning("Part size multiplier must be >= 0."
                               " Defaulting to 10")
                self.part_size_multiplier = 10
            self.part_size_multiplier = part_size_multiplier

        self.all_keys = set()  # Keys the user adds
        self.keys_to_delete = set()  # Keys to delete on cleanup
        self.remove_keys = remove_keys
        self.file_cache = []  # io objects that are ready to be combined
        self.cache_size = cache_size
        if self.cache_size is None or self.cache_size <= 0:
            raise ValueError("cache size must be 1 or larger")

        self.s3_max_retries = s3_max_retries
        if self.s3_max_retries is None or self.s3_max_retries <= 0:
            raise ValueError("s3 max retries must be 1 or larger")

        self.s3 = _create_s3_client(
            session,
            pool_size=self.cache_size * 2,
            max_retries=self.s3_max_retries,
        )

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

        self._cleanup()

    def _cleanup(self):
        """Remove source keys from s3
        """
        if self.remove_keys is True:
            logger.info("Removing all keys added to the tar {filename}"
                        .format(filename=self.target_key))

            while len(self.keys_to_delete) > 0:
                delete_these = []
                while len(delete_these) < 1000 and len(self.keys_to_delete) > 0:
                    delete_these.append({'Key': self.keys_to_delete.pop()[1]})
                logger.debug("Removing {} keys from {}"
                             .format(len(delete_these), self.source_bucket))
                resp = self.s3.delete_objects(
                    Bucket=self.source_bucket,
                    Delete={'Objects': delete_these}
                )
                logger.debug("Delete objects response: {}".format(resp))

        # TODO: Clear the whole class
        self.s3 = None  # Clear all current connections

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
        while current_size < MIN_S3_SIZE * self.part_size_multiplier:
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
        return self.all_keys == set() and self.file_cache == []

    def _pre_fetch_files(self):
        """Started as a background job to keep adding files to the
        file cache to speed things along
        """
        def _fetch(item):
            while len(self.file_cache) >= self.cache_size:
                # Hold here until more files are needed in the cache
                time.sleep(0.1)
            tar_member_name, key = item
            logger.debug("Adding to cache {}".format(key))
            self._add_key_to_cache(tar_member_name, key)

        _threads(self.cache_size, self.all_keys, _fetch)
        self.keys_to_delete = self.all_keys.copy()
        self.all_keys = set()  # clear now that all have been processed

    def _add_key_to_cache(self, tar_member_name, key):
        """Get the source of an s3 key (and its metadata if needed) and add
        it to the file cache

        Args:
            tar_member_name (str): Filename and path of the file inside the tar
            key (str): the key to download form s3
        """
        if self.save_metadata is True:
            metadata_io = self._get_tar_source_metadata(tar_member_name, key)
            if metadata_io is not None:
                logger.debug("Adding metadata file to cache {}".format(key))
                self.file_cache.append(metadata_io)

        self.file_cache.append(self._get_tar_source_data(tar_member_name, key))

    def _get_tar_source_data(self, tar_member_name, key):
        """Download source file and generate a tar from it

        Args:
            tar_member_name (str): Filename and path of the file inside the tar
            key (str): File from s3 to download

        Returns:
            io.BytesIO: BytesIO object of the tar file
        """
        source_key_io = self._download_source_file(key)
        source_mtime = self._get_source_key_mtime(key)
        source_tar_io = self._save_bytes_to_tar(
            tar_member_name,
            source_key_io,
            source_mtime,
            mode=self.mode,
        )
        source_key_io.close()  # Cleanup
        return source_tar_io

    def _get_tar_source_metadata(self, tar_member_name, key):
        """Get metadata from the s3 file
        If a file has metadata then add it to a tar file
        If not, then return None

        Args:
            tar_member_name (str): Filename and path of the file inside the tar
            key (str): S3 file to get metadata from

        Returns:
            io.BytesIO|None: BytesIO object of the tar file
        """
        source_metadata_io = self._download_source_metadata(key)
        if source_metadata_io is None:
            return None

        source_metadata_tar_io = self._save_bytes_to_tar(
            tar_member_name + '.metadata.json',
            source_metadata_io,
            time.time(),
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

    def _get_source_key_mtime(self, key):
        return self.s3.head_object(
            Bucket=self.source_bucket,
            Key=key,
        )['LastModified'].timestamp()

    @classmethod
    def _save_bytes_to_tar(cls, name, source_io, source_mtime, mode):
        """Convert raw bytes into a tar

        Args:
            name (str): Filename inside the tar
            source_io (io.BytesIO): The data to be saved into the tar
            source_mtime (): Last modified timestamp of the source file
            mode (str): The file mode in which to open the tar file

        Returns:
            io.BytesIO: BytesIO object of the tar'd data
        """
        source_tar_io = io.BytesIO()
        tar = tarfile.open(fileobj=source_tar_io, mode=mode)
        info = tarfile.TarInfo(name=name)
        info.size = source_io.tell()
        info.mtime = source_mtime
        source_io.seek(0)
        tar.addfile(tarinfo=info, fileobj=source_io)

        if '|' in mode:
            # When using compression, the data is
            # not writted to the fileobj until its done
            tar.fileobj.close()

        return source_tar_io

    def _raise_if_dup(self, tar_member_name, key, key_list=None):
        if key_list is None:
            key_list = self.all_keys

        if self.allow_dups is False:
            if any((True for x in key_list
                    if tar_member_name == x[0] and key != x[1])):
                raise ValueError(("Filename '{member_name}' for key '{key}'"
                                  " already exists in the tar file."
                                  " Set allow_dups to continue.")
                                 .format(member_name=tar_member_name,
                                         key=key))

    def add_files(self, prefix, folder='', preserve_paths=False):
        """Add s3 files from a directory inside the source bucket

        self.all_keys gets added to directly

        Args:
            prefix (str): Folder path inside the source bucket
            folder (str, optional): Folders to place the file inside
                the tar file. Defaults to ''.
            preserve_paths (bool, optional): Starting from the prefix, use
                the path of the key in the tar file. Defaults to False.
        """
        def resp_to_filelist(resp):
            file_list = []
            for x in resp['Contents']:
                key = x['Key']
                if key == prefix:
                    continue

                # Get the paths after the prefix, and before the file
                if preserve_paths is True:
                    tar_member_name = folder + key.replace(prefix, '')
                else:
                    tar_member_name = folder + key.split('/')[-1]

                self._raise_if_dup(tar_member_name, key, key_list=self.all_keys)
                self._raise_if_dup(tar_member_name, key, key_list=file_list)

                file_list.append((tar_member_name, key))

            return file_list

        # needs to end with '/'
        if folder != '' and not folder.endswith('/'):
            folder += '/'

        logger.info("Gathering files from folder {}".format(prefix))
        resp = self.s3.list_objects_v2(Bucket=self.source_bucket, Prefix=prefix)
        if resp['KeyCount'] == 0:
            logger.warning("No files found in the prefix {}".format(prefix))
            return

        file_list = resp_to_filelist(resp)
        self.all_keys |= set(file_list)
        total_file_count = len(file_list)

        logger.debug("Found {} objects so far...".format(total_file_count))
        while resp['IsTruncated']:
            last_key = file_list[-1][1]
            resp = self.s3.list_objects(
                Bucket=self.source_bucket,
                Prefix=prefix,
                Marker=last_key,
            )
            file_list = resp_to_filelist(resp)
            self.all_keys |= set(file_list)
            total_file_count += len(file_list)

            logger.debug("Found {} objects so far...".format(total_file_count))

        logger.info("Found {} objects under the prefix '{}'"
                    .format(total_file_count, prefix))

    def add_file(self, key, folder=''):
        """Add a single file at a time to be tar'd

        self.all_keys gets added to directly

        Args:
            key (str): The full path to a single s3 file in the source bucket
            folder (str, optional): Folders to place the file inside
                the tar file. Defaults to ''.
        """
        # TODO make sure key exists, if not log error and do not break
        if folder != '' and not folder.endswith('/'):
            folder += '/'
        tar_member_name = folder + key.split('/')[-1]

        self._raise_if_dup(tar_member_name, key)

        self.all_keys.add((tar_member_name, key))
