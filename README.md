# s3-tar

[![PyPI](https://img.shields.io/pypi/v/s3-tar.svg)](https://pypi.python.org/pypi/s3-tar)
[![PyPI](https://img.shields.io/pypi/l/s3-tar.svg)](https://pypi.python.org/pypi/s3-tar)  


Create a `tar`/`tar.gz`/`tar.bz2` file from many s3 files and stream back into s3.   

## Install
`pip install s3-tar`


## Usage

Set up s3 credentials on your system by either of these options:
- Environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- Shared credential file `~/.aws/credentials`
- AWS config file `~/.aws/config`
For details check out the aws docs: https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html  

Set the environment variable `S3_ENDPOINT_URL` to use a custom s3 host (minio/etc...) , not needed if using AWS s3. 

This will use very little RAM. As it downloads files, it streams up the tar'd pieces as it goes.  
You can use more or less ram by playing with the options `cache_size` & `part_size_multiplier`.  



### Import
```python
from s3_tar import S3Tar

# Init the job
job = S3Tar(
    'YOUR_BUCKET_NAME',
    'FILE_TO_SAVE_TO.tar',  # Use `tar.gz` or `tar.bz2` to enable compression
    # target_bucket=None,  # Default: source bucket. Can be used to save the archive into a different bucket
    # min_file_size='50MB',  # Default: None. The min size to make each tar file [B,KB,MB,GB,TB]. If set, a number will be added to each file name
    # save_metadata=False,  # If True, and the file has metadata, save a file with the same name using the suffix of `.metadata.json`
    # remove_keys=False,  # If True, will delete s3 files after the tar is created
  
    # ADVANCED USAGE
    # allow_dups=False,  # When False, will raise ValueError if a file will overwrite another in the tar file, set to True to ignore
    # cache_size=5,  # Default 5. Number of files to hold in memory to be processed
    # s3_max_retries=4,  # Default is 4. This value is passed into boto3.client's s3 botocore config as the `max_attempts`
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


### Command Line
To see all command line options run:  
```
s3-tar -h                                                       
usage: s3-tar [-h] --source-bucket SOURCE_BUCKET --folder FOLDER --filename FILENAME [--target-bucket TARGET_BUCKET] [--min-filesize MIN_FILESIZE] [--save-metadata] [--remove]
              [--preserve-paths] [--allow-dups] [--cache-size CACHE_SIZE] [--s3-max-retries S3_MAX_RETRIES] [--part-size-multiplier PART_SIZE_MULTIPLIER]

Tar (and compress) files in s3

optional arguments:
  -h, --help            show this help message and exit
  --source-bucket SOURCE_BUCKET
                        base bucket to use
  --folder FOLDER       folder whose contents should be combined
  --filename FILENAME   Output filename for the tar file. Extension: tar, tar.gz, or tar.bz2
  --target-bucket TARGET_BUCKET
                        Bucket that the tar will be saved to. Only needed if different then source bucket
  --min-filesize MIN_FILESIZE
                        Use to create multiple files if needed. Min filesize of the tar'd files in [B,KB,MB,GB,TB]. e.x. 5.2GB
  --save-metadata       If a file has metadata, save it to a .metadata.json file
  --remove              Delete files that were added to the tar file
  --preserve-paths      Preserve the path layout relative to the input folder
  --allow-dups          ADVANCED: Allow duplicate filenames to be saved into the tar file
  --cache-size CACHE_SIZE
                        ADVANCED: Number of files to download into memory at a time
  --s3-max-retries S3_MAX_RETRIES
                        ADVANCED: Max retries for each request the s3 client makes
  --part-size-multiplier PART_SIZE_MULTIPLIER
                        ADVANCED: Multiplied by 5MB to set the max size of each upload chunk
  --storage-class STORAGE_CLASS
                        ADVANCED: Storage class selector (Defaults to 'STANDARD', see s3 documentation for valid choices)
```


#### CLI Examples
This example will take all the files in the bucket `my-data` in the folder `2020/07/01` and save it into a compressed tar gzip file in the same bucket into the directory `Archives` 
```
s3-tar --source-bucket my-data --folder 2020/07/01 --filename Archive/2020-07-01.tar.gz
```

Now lets say you have a large amount of data and it would create a tar file to large to work with. This example will create files that are ~2.5GB each and save into a different bucket. Inside each tar file it will also save the folder structure as it is in s3.
```
s3-tar --preserve-paths --source-bucket my-big-data --folder 2009 --target-bucket my-archived-data --filename big_data/2009-archive.tar.gz --min-filesize 2.5GB
```
In the bucket `my-archived-data`, in the folder `big_data/` there will be multiple files named:
- 2009-archive-1.tar.gz
- 2009-archive-2.tar.gz
- 2009-archive-3.tar.gz


#### Notes

- For better performance, if you know the files you are adding will not have any duplicate names (or you are ok with duplicates), you can set `--allow-dups` in the cli or pass `allow_dups=True` to the `S3Tar` class to get better performance since it wil not have to check each files name.
