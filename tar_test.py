import io
import boto3
import tarfile

BUCKET = 'eddys-sandbox'
TAR_FILE = 'archive/test.tar.gz'

# Add files
KEYS = [
    'data/a/cookies.txt',
    'data/a/aholisticapproachtolessonslearned.pdf',
    'data/a/freenas-FreeNAS-11.2-U6-20200127065525.tar',
]


tar = tarfile.open('normal.tar.gz', "w|gz")
string = io.BytesIO()
string.write(b"hello")
info = tarfile.TarInfo(name="foo.txt")
info.size = string.tell()
# print("Stream size", info.size)
string.seek(0)
# print(info)
tar.addfile(tarinfo=info, fileobj=string)
string = io.BytesIO()
string.write(b"world")
info = tarfile.TarInfo(name="bar.txt")
info.size = string.tell()
string.seek(0)
tar.addfile(tarinfo=info, fileobj=string)
tar.close()


noraml_tar = tarfile.open('normal.tar.gz', 'r')
print(noraml_tar.getmembers())


###
# Test streaming the tar.gz
###
test_a = io.BytesIO()
tar = tarfile.open(fileobj=test_a, mode="w|gz")
string = io.BytesIO()
string.write(b"hello")
info = tarfile.TarInfo(name="foo.txt")
info.size = string.tell()
# print("Stream size", info.size)
string.seek(0)
# print(info)
tar.addfile(tarinfo=info, fileobj=string)

# Only close the first file
# tar.close()


test_b = io.BytesIO()
tar = tarfile.open(fileobj=test_b, mode="w:gz")
string = io.BytesIO()
string.write(b"world")
info = tarfile.TarInfo(name="bar.txt")
info.size = string.tell()
string.seek(0)
tar.addfile(tarinfo=info, fileobj=string)

# * Last file has to close (only needed for gz, not normal tar)
tar.close()

test_a.seek(0)
test_b.seek(0)

new_io = io.BytesIO()
new_io.write(test_a.read())
new_io.write(test_b.read())
new_io.seek(0)

session = boto3.session.Session()
s3 = session.client('s3')

s3.put_object(Bucket=BUCKET, Key=TAR_FILE, Body=new_io.read())

new_io.seek(0)
streamed_tar = tarfile.open(fileobj=new_io, mode='r')
print(streamed_tar.getmembers())
