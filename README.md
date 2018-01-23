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

## License and Copyright

All material in this repository are Copyright 2002-2018 Hitachi Vantara. All code is licensed as Apache 2.0 unless explicitly stated. See the LICENSE file for more details.

## Support Statement

This work is at Stage 1 : Development Phase: Start-up phase of an internal project. Usually a Labs experiment. (Unsupported)