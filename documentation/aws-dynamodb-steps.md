[Home](../readme.md)

# AWS DynamoDB Custom Steps for PDI

Included:-
- AWS DynamoDB Database Connection and Wizard - Creates a DynamoDB connection
- AWS DynamoDB Input Step - Loads JSON documents from DynamoDB
- AWS DynamoDB Output Step - Saves JSON documents to DynamoDB

## The DynamoDB Steps

Included are steps to read an entire database's contents as JSON files, and to write JSON files to a database on DynamoDB.

## Installation and Usage

See the main page for installation and usage

## Features

-	DynamoDB Database Connection type and wizard
-	DynamoDB Output step – takes all fields in the transformation and saves in to a given record. First field assumed to be the (only) primary key field. Stored as typed items in the record
-	DynamoDB Scan Input step – Scans an entire DynamoDB table, retrieving all Fields in the stream (I.e. if you have a field called ‘MyKey’ and ‘Amount’, the scan will only pull back those fields from the record, not all fields)
-	Uses batching for both of the above operations (25 record limit per batched request on write)
-	Received approx. 350 records written per second on testing on my laptop talking to a remote DynamoDB instance – it should be noted the Free Tier of DynamoDB has Concurrent Write limits imposed, so maximum speed is likely much, much higher
-	Theoretically supports writing to and querying records in multiple DynamoDB tables due to the TableName being implemented as a field in the stream, not hardcoded, but this wasn’t tested. You probably want to combine this with a Sort before the DynamoDB Output Step to get the best batching performance.

## Limitations

-	Doesn’t handle JSON fields (only strings – JSON can be handled natively in DynamoDB Items)
-	Only supports a single primary key field
-	Assumes the first field in the stream is the key field (use select values to reorder)
-	Assumes all fields in the stream should be written to/read from DynamoDB (use select values to limit fields before these steps)

## Improvements

If you wish a new feature was added, or have a question, please [log an issue](https://github.com/Pentaho-SE-EMEA-APAC/pdi-aws/issues/new) on GitHub.