import logging

logger = logging.getLogger(__name__)


class S3MPU:

    def __init__(self, s3, target_bucket, target_key):
        self.s3 = s3
        self.target_bucket = target_bucket
        self.target_key = target_key
        self.parts_mapping = []

        logger.info("Creating file {}".format(self.target_key))
        self.resp = self.s3.create_multipart_upload(
            Bucket=self.target_bucket,
            Key=self.target_key,
        )

    def upload_part(self, source_io):
        part_num = len(self.parts_mapping) + 1
        logger.debug("Uploading part {} of {}"
                     .format(part_num, self.target_key))

        source_io.seek(0)
        part_resp = self.s3.upload_part(
            Bucket=self.target_bucket,
            Key=self.target_key,
            PartNumber=part_num,
            UploadId=self.resp['UploadId'],
            Body=source_io.read(),
        )
        source_io.close()  # Cleanup

        self.parts_mapping.append({
            'ETag': part_resp['ETag'][1:-1],
            'PartNumber': part_num,
        })

    def complete(self):
        # Finish
        self.s3.complete_multipart_upload(
            Bucket=self.target_bucket,
            Key=self.target_key,
            UploadId=self.resp['UploadId'],
            MultipartUpload={'Parts': self.parts_mapping},
        )
