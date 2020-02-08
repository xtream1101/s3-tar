import io
import os
import boto3
import tarfile


KB = 1024
MB = KB**2
GB = KB**3
TB = KB**4
MIN_S3_SIZE = 5 * MB

BUCKET = 'eddys-sandbox'
TAR_FILE = 'archive/multi_part_test.tar.gz'


# Add files
KEYS = [
    'data/a/cookies.txt',
    'data/a/aholisticapproachtolessonslearned.pdf',
    'data/a/freenas-FreeNAS-11.2-U6-20200127065525.tar',
]


session = boto3.session.Session()
s3 = session.client('s3', endpoint_url=os.getenv('S3_ENDPOINT_URL'))


# Start multipart upload
resp = s3.create_multipart_upload(
    Bucket=BUCKET,
    Key=TAR_FILE,
)


parts_mapping = []
part_num = 0
while KEYS:
    part_num += 1
    print(f"Creating part {part_num}")
    current_size = 0
    current_part_io = io.BytesIO()
    while KEYS != [] and current_size < MIN_S3_SIZE:
        key = KEYS.pop()
        print(f"Adding key {key}")
        source_key_io = io.BytesIO()
        r = s3.download_fileobj(BUCKET, key, source_key_io)
        # print(f"Download size {source_key_io.tell()}")

        source_tar_io = io.BytesIO()
        tar = tarfile.open(fileobj=source_tar_io, mode="w|gz")
        info = tarfile.TarInfo(name=key.split('/')[-1])
        info.size = source_key_io.tell()
        source_key_io.seek(0)
        tar.addfile(tarinfo=info, fileobj=source_key_io)

        if len(KEYS) == 0:  # This is the last one pop'd off
            print("close tar file")
            tar.close()  # Append what the tar.gz EOF should look like
        else:
            tar.fileobj.close()  # Write the data to the io object

        # print(f"source tar size: {source_tar_io.tell()}")
        source_tar_io.seek(0)
        current_part_io.write(source_tar_io.read())
        # print(f"curr size: {current_part_io.tell()}")
        current_size = current_part_io.tell()

    print(f"Upload part {part_num}")
    current_part_io.seek(0)
    part_resp = s3.upload_part(
        Bucket=BUCKET,
        Key=TAR_FILE,
        PartNumber=part_num,
        UploadId=resp['UploadId'],
        Body=current_part_io.read(),
    )

    parts_mapping.append({
        'ETag': part_resp['ETag'][1:-1],
        'PartNumber': part_num,
    })


# Finish
parts = {'Parts': parts_mapping}
s3.complete_multipart_upload(
    Bucket=BUCKET,
    Key=TAR_FILE,
    UploadId=resp['UploadId'],
    MultipartUpload=parts,
)
