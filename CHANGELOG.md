# Change log

### 0.1.2
- Fixed memory bug where files were download files were not getting cleaned up properly


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
