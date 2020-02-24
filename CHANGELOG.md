# Change log


### 0.1.6
- Option to preserve folder structure. Also add files to a custom folder 
- Fixed connection pool bug to s3


### 0.1.5
- Added `--remove` which when set will delete all files that were added to the tar file
- Refactored the S3Tar class to be easier to work with/test
- Added tests


### 0.1.4
- Fixed bug where the base folder is added to the tar with no filename
- Added `--save-metadata`. Will save any metadata a file has to a json file of the same name with the suffix `.metadata.json`


### 0.1.3
- Added threads
    - adds the cli flag `--cache-size` which is the number of files to download and store in memory


### 0.1.2
- Fixed memory bug where files that were download were not getting cleaned up properly


### 0.1.1
- Fix bug when looping over a folder with many files
- More output when setting the env var `S3_TAR_LOG_LEVEL=debug`


### 0.1.0
- Added support for a different target bucket
    - Cli args changes
        - `--bucket` --> `--source-bucket`
        - Added `--target-bucket`
- Added support for `.tar.bz2` archives


### 0.0.1
- Converted to a pypi package and added cli command
