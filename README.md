# AWS Addons for Pentaho Data Integration

This repository holds various add-on functionality to enable Pentaho Data Integration (PDI) to better
work with Amazon Web Services (AWS).

## Current contents

- [AWS S3 VFS Driver](documentation/aws-s3-vfs-driver.md) - So any PDI step can store and read files to/from AWS S3 (Simple Storage Service)
- [AWS DynamoDB Steps](documentation/aws-dynamodb-steps.md) - So a PDI transformation can connect to DynamoDB and save data to it, or load data from it

## Installation and usage

Easiest way is to download the binary release.

- Fetch the latest release from here: https://github.com/Pentaho-SE-EMEA-APAC/pdi-aws/releases 
- Unzip the zip file
- Take the 'pdi-aws' folder and drop in to PENTAHO_HOME/design-tools/data-integration/plugins
- Download the AWS DynamoDB Java SDK zip file from here: http://TODO.com/
- Extract the contents of the zip file
- Copy the lib/*.jar files - to the plugins/pdi-aws/lib folder (create this subfolder if it does not exist)
- Restart PDI/Spoon
- You will find the AWS DynamoDB steps under the 'Big Data' folder. The AWS S3 VFS driver will be automatically enabled.

## Samples

Sample transforms are available in the ZIP release, within the samples folder.

## License and Copyright

All material in this repository are Copyright 2002-2018 Hitachi Vantara. All code is licensed as Apache 2.0 unless explicitly stated. See the LICENSE file for more details.

## Support Statement

This work is at Stage 1 : Development Phase: Start-up phase of an internal project. Usually a Labs experiment. (Unsupported)
