from s3_tar import S3Tar


job = S3Tar('eddys-sandbox', 'archive/class_test2.tar.gz', '10MB')
# job.add_file('data/a/cookies.txt')
# job.add_file('data/a/aholisticapproachtolessonslearned.pdf')
job.add_files('data/a')
job.tar()
