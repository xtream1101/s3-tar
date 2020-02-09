# s3-tar

[![PyPI](https://img.shields.io/pypi/v/s3-tar.svg)](https://pypi.python.org/pypi/s3-tar)
[![PyPI](https://img.shields.io/pypi/l/s3-tar.svg)](https://pypi.python.org/pypi/s3-tar)  


Create a `tar`/`tar.gz`/`tar.bz2` file from many s3 files and stream back into s3.  
_*Currently does not preserve directory structure, all files will be in the root dir_  

## Install
`pip install s3-tar`


## Usage

### Command Line
`$ s3-tar -h`

### Import
```python
from s3_tar import S3Tar

bucket = 'YOUR_BUCKET_NAME'
path_to_tar = 'PATH_TO_FILES_TO_CONCAT'
tared_file = 'FILE_TO_SAVE_TO.tar'  # use `tar.gz` or `tar.bz2` to enable compression
# Setting this to a size will always add a part number at the end of the file name
min_file_size = '50MB'  # ex: FILE_TO_SAVE_TO-1.tar, FILE_TO_SAVE_TO-2.tar, ...
# Setting this to None will create a single tar with all the files
# min_file_size = None

# Init the job
job = S3Tar(bucket, tared_file,
            min_file_size=min_file_size,
            target_bucket=None,  # Can be used to save the archive into a different bucket
            # session=boto3.session.Session(),  # For custom aws session
)
# Add files, can call multiple times to add files from other directories
job.add_files(path_to_concat)
# Add a single file at a time
job.add_file('some/file_key.json')
# Star the tar'ing job after files have been added
job.tar()
```
