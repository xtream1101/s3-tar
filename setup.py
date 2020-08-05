from setuptools import setup


with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='s3-tar',
    packages=['s3_tar'],
    version='0.1.13',
    description='Tar (and compress) files in s3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Eddy Hintze',
    author_email="eddy@hintze.co",
    url="https://github.com/xtream1101/s3-tar",
    license='MIT',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
    ],
    entry_points={
        'console_scripts': [
            's3-tar=s3_tar.cli:cli',
        ],
    },
    install_requires=[
        'boto3',
    ],

)
