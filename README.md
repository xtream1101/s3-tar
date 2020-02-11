# s3-tar

[![PyPI](https://img.shields.io/pypi/v/s3-tar.svg)](https://pypi.python.org/pypi/s3-tar)
[![PyPI](https://img.shields.io/pypi/l/s3-tar.svg)](https://pypi.python.org/pypi/s3-tar)  


Create a `tar`/`tar.gz`/`tar.bz2` file from many s3 files and stream back into s3.  
_*Currently does not preserve directory structure, all files will be in the root dir_  

## Install
`pip install s3-tar`


## Usage

### Command Line
To see all command line options run:  
`s3-tar -h`


### Import
```python
from s3_tar import S3Tar

# Init the job
job = S3Tar(
    'YOUR_BUCKET_NAME',
    'FILE_TO_SAVE_TO.tar',  # Use `tar.gz` or `tar.bz2` to enable compression
    # min_file_size='50MB',  # The min size to make each tar file [B,KB,MB,GB,TB]. If set, a number will be added to each file name
    # target_bucket=None,  # Default: source bucket. Can be used to save the archive into a different bucket
    # cache_size=5,  # Default 5, Number of files to hold in memory to be processed
    # save_metadata=False,  # If True
    # session=boto3.session.Session(),  # For custom aws session
)
# Add files, can call multiple times to add files from other directories
job.add_files('FOLDER_IN_S3/)
# Add a single file at a time
job.add_file('some/file_key.json')
# Star the tar'ing job after files have been added
job.tar()
```
