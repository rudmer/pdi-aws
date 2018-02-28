/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.core.database.aws.dynamodb;

import org.pentaho.di.core.database.aws.common.AWSAuthScheme;
import org.pentaho.di.core.database.aws.common.AWSDeploymentType;

import org.pentaho.di.core.Const;
import org.pentaho.di.core.database.DatabaseFactoryInterface;
import org.pentaho.di.core.database.DatabaseMeta;
import org.pentaho.di.core.exception.KettleDatabaseException;
import org.pentaho.di.i18n.BaseMessages;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

/**
 * A ConnectionFactory for AWS DynamoDB
 * 
 * @author Adam Fowler <adam.fowler@hitachivantara.com>
 * @since 28-02-2018
 */
public class DynamoDBConnectionFactory implements DatabaseFactoryInterface {

  public static DynamoDB create( String host, int port, String awsRegion, AWSAuthScheme authScheme, AWSDeploymentType deployment, String username, String password, String databaseName) throws Exception {
    AmazonDynamoDBClientBuilder builder = AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(
      new AwsClientBuilder.EndpointConfiguration("https://" + host + ":" + port, awsRegion));
    builder.setCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(username,password)));
    AmazonDynamoDB client = builder.build(); // TODO check whether this should be https
    return new DynamoDB(client); // TODO specify default database
  }

  /**
   * The DynamoDB connection to test
   */
  public String getConnectionTestReport(DatabaseMeta databaseMeta) throws KettleDatabaseException {

    StringBuilder report = new StringBuilder();

    // TODO actually test this

    return report.toString();
  }
}
