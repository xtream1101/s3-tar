# s3-tar

[![PyPI](https://img.shields.io/pypi/v/s3-tar.svg)](https://pypi.python.org/pypi/s3-tar)
[![PyPI](https://img.shields.io/pypi/l/s3-tar.svg)](https://pypi.python.org/pypi/s3-tar)  


Create a `tar`/`tar.gz`/`tar.bz2` file from many s3 files and stream back into s3.   

## Install
`pip install s3-tar`


## Usage

Set the environment variable `S3_ENDPOINT_URL` to use a custom s3 host (minio/etc...)  

This will use very little RAM. As it downloads files, it streams up the tar'd pieces as it goes.  
You can use more or less ram by playing with the options `cache_size` & `part_size_multiplier`.  


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
    # target_bucket=None,  # Default: source bucket. Can be used to save the archive into a different bucket
    # min_file_size='50MB',  # Default: None. The min size to make each tar file [B,KB,MB,GB,TB]. If set, a number will be added to each file name
    # cache_size=5,  # Default 5. Number of files to hold in memory to be processed
    # save_metadata=False,  # If True, and the file has metadata, save a file with the same name using the suffix of `.metadata.json`
    # remove_keys=False,  # If True, will delete s3 files after the tar is created
    # allow_dups=False,  # When False, will raise ValueError if a file will overwrite another in the tar file, set to True to ignore
    # part_size_multiplier=10,  # is multiplied by 5 MB to find how large each part that gets upload should be
    # session=boto3.session.Session(),  # For custom aws session
)
# Add files, can call multiple times to add files from other directories
job.add_files(
    'FOLDER_IN_S3/',
    # folder='',  # If a folder is set, then all files from this directory will be added into that folder in the tar file
    # preserve_paths=False,  # If True, it will use the dir paths relative to the input path inside the tar file
)
# Add a single file at a time
job.add_file(
    'some/file_key.json',
    # folder='',  # If a folder is set, then the file will be added into that folder in the tar file
)
# Start the tar'ing job after files have been added
job.tar()
```
