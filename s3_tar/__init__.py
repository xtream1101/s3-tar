import io
import boto3
import logging
import tarfile
from .utils import _create_s3_client, _convert_to_bytes, MIN_S3_SIZE

logger = logging.getLogger(__name__)


class S3Tar:

    def __init__(self, bucket, key, min_file_size,
                 session=boto3.session.Session()):
        self.bucket = bucket
        # TODO: key must end in .tar or .tar.gz, this is how to know what compression to use
        # TODO: Test other types of compressions
        self.key = key
        # TODO: have min size optional, if none then do not have file_number in name
        self.min_file_size = _convert_to_bytes(min_file_size)
        self.content_type = 'application/gzip'  # for uncompressed tar application/x-tar
        self.all_keys = []
        self.s3 = _create_s3_client(session)

    def tar(self):
        file_number = 0
        # Keep creating new tar(.gz) as long as there are files left
        while self.all_keys != []:
            file_number += 1
            print(f"Creating file number {file_number}")
            if '.tar' in self.key.split('/')[-1]:
                # If there is a file extention, put the part number before it
                path_parts = self.key.rsplit('.tar', 1)
                result_filepath = '{}-{}.tar{}'.format(
                    path_parts[0],
                    file_number,
                    path_parts[1],
                )
            else:
                result_filepath = '{}-{}'.format(self.key, file_number)

            # Start multipart upload
            resp = self.s3.create_multipart_upload(
                Bucket=self.bucket,
                Key=result_filepath,
            )

            parts_mapping = []
            part_num = 0
            current_file_size = 0
            current_part_io = io.BytesIO()
            # If out of files or min size is met, then complete file
            while self.all_keys != [] and current_file_size < self.min_file_size:
                part_num += 1
                print(f"Creating part {part_num}")
                current_part_size = 0
                while self.all_keys != [] and current_part_size < MIN_S3_SIZE*2:
                    key = self.all_keys.pop()
                    print(f"Adding key {key}")
                    source_key_io = io.BytesIO()
                    self.s3.download_fileobj(self.bucket, key, source_key_io)

                    source_tar_io = io.BytesIO()
                    tar = tarfile.open(fileobj=source_tar_io, mode="w|gz")
                    info = tarfile.TarInfo(name=key.split('/')[-1])
                    info.size = source_key_io.tell()
                    source_key_io.seek(0)
                    tar.addfile(tarinfo=info, fileobj=source_key_io)

                    if len(self.all_keys) == 0:  # This is the last one pop'd off
                        print("close tar file")
                        tar.close()  # Append what the tar.gz EOF should look like
                    else:
                        tar.fileobj.close()  # Write the data to the io object

                    # print(f"source tar size: {source_tar_io.tell()}")
                    source_tar_io.seek(0)
                    current_part_io.write(source_tar_io.read())
                    # print(f"curr size: {current_part_io.tell()}")
                    current_part_size = current_part_io.tell()

                current_file_size += current_part_size

                print(f"Upload part {part_num}")
                current_part_io.seek(0)
                part_resp = self.s3.upload_part(
                    Bucket=self.bucket,
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
                Bucket=self.bucket,
                Key=result_filepath,
                UploadId=resp['UploadId'],
                MultipartUpload={'Parts': parts_mapping},
            )

    def add_files(self, prefix):

        def resp_to_filelist(resp):
            return [(x['Key']) for x in resp['Contents']]

        objects_list = []
        resp = self.s3.list_objects(Bucket=self.bucket, Prefix=prefix)
        objects_list.extend(resp_to_filelist(resp))
        logger.info("Found {} objects so far".format(len(objects_list)))
        while resp['IsTruncated']:
            last_key = objects_list[-1][0]
            resp = self.s3.list_objects(
                Bucket=self.bucket,
                Prefix=prefix,
                Marker=last_key,
            )
            objects_list.extend(resp_to_filelist(resp))
            logger.info("Found {} objects so far".format(len(objects_list)))

        self.all_keys.extend(objects_list)

    def add_file(self, key):
        # TODO make sure key exists, if not log error and do not break
        self.all_keys.append(key)
