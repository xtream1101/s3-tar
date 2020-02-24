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
        logger.debug("Multipart upload start: {}".format(self.resp))

    def upload_part(self, source_io):
        """Upload a part of the multipart upload

        Save to a parts mapping, needed to complete the multipart upload

        Args:
            source_io (io.BytesIO): BytesIO object to upload

        Returns:
            bool: If the upload was successful
        """

        part_num = len(self.parts_mapping) + 1
        logger.info("Uploading part {} of {}"
                    .format(part_num, self.target_key))

        source_io.seek(0)
        resp = self.s3.upload_part(
            Bucket=self.target_bucket,
            Key=self.target_key,
            PartNumber=part_num,
            UploadId=self.resp['UploadId'],
            Body=source_io.read(),
        )
        source_io.close()  # Cleanup
        logger.debug("Multipart upload part: {}".format(resp))

        resp_status_code = resp['ResponseMetadata']['HTTPStatusCode']
        if resp_status_code == 200:
            self.parts_mapping.append({
                'ETag': resp['ETag'],
                'PartNumber': part_num,
            })
            return True

        return False

    def complete(self):
        """Complete to multipart upload in s3

        Returns:
            bool: If the upload was successful
        """
        resp = self.s3.complete_multipart_upload(
            Bucket=self.target_bucket,
            Key=self.target_key,
            UploadId=self.resp['UploadId'],
            MultipartUpload={'Parts': self.parts_mapping},
        )
        logger.debug("Multipart upload complete: {}".format(resp))
        return resp['ResponseMetadata']['HTTPStatusCode'] == 200
