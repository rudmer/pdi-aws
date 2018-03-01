[Home](../readme.md)

# Amazon S3 Commons VFS driver

This is an AWS S3 vfs driver built using the latest AWS SDK.

## Installation

Simply unzip the release zip onto PDI plugins folder and restart the Spoon UI.

## Usage

### Filesystem options

All the below are optional, when they are not provided for authentication, the VFS driver uses the AWS SDK default credentials chain

Option | Description | Default
------------ | ------------- | -------------
endpoint | customize s3 endpoint | N/A 
region | region to connect to | Regions.DEFAULT_REGION
accessKeyId | an explicit reference to the S3 access key | N/A 
secretAccessKey | an explicit reference to the S3 secret key | N/A
encryptionMethod | One of: NONE, CLIENT_SIDE, SERVER_SIDE | NONE
kms.keyAlias | the name of an AWS KMS key to use for encryption | N/A

#### Using the filesystem options via PDI

Using kettle.properties
```
vfs.s3sdk.endpoint=
vfs.s3sdk.region=
vfs.s3sdk.accessKeyId=
vfs.s3sdk.secretAccessKey=
vfs.s3sdk.encryptionMethod=
vfs.s3sdk.kms.keyAlias=
```

### Authentication

All the available AWS SDK means to provide credentials are available to use with this vfs driver.

#### EXAMPLE: Using .aws/credentials

```
[default]
aws_access_key_id=
aws_secret_access_key=
```

### VFS URI

```
s3sdk://bucket/path/to/file.txt
```

## License and Copyright

All material in this repository are Copyright 2002-2018 Hitachi Vantara. All code is licensed as Apache 2.0 unless explicitly stated. See the LICENSE file for more details.

## Support Statement

This work is at Stage 1 : Development Phase: Start-up phase of an internal project. Usually a Labs experiment. (Unsupported)