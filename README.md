# Amazon S3 Commons VFS driver

This is an AWS S3 vfs driver built using the latest AWS SDK.

## Usage

### Authentication

All the available AWS SDK means to provide credentials are available to use with this vfs driver.

Example:
.aws/credentials
```
[default]
aws_access_key_id=< access key >
aws_secret_access_key=<secret key >
```

### VFS URI

```
s3sdk://(region:)bucket/path/to/file.txt
```