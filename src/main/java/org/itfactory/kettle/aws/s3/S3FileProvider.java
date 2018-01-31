/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2018 by Pentaho : http://www.pentaho.com
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

package org.itfactory.kettle.aws.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kms.AWSKMSClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Builder;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3EncryptionClientBuilder;
import com.amazonaws.services.s3.model.CryptoConfiguration;
import com.amazonaws.services.s3.model.CryptoMode;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * VFS FileProvider implementation for S3
 *
 * @author asimoes
 * @since 09-11-2017
 */
public class S3FileProvider extends AbstractOriginatingFileProvider {

  public static final String SCHEME = "s3sdk";
  protected static final Collection<Capability> capabilities =
    Collections.unmodifiableCollection(
      Arrays.asList(
        new Capability[] {
          Capability.CREATE,
          Capability.DELETE,
          Capability.RENAME,
          Capability.GET_TYPE,
          Capability.LIST_CHILDREN,
          Capability.READ_CONTENT,
          Capability.URI,
          Capability.WRITE_CONTENT,
          Capability.GET_LAST_MODIFIED,
          Capability.RANDOM_ACCESS_READ
        } ) );

  public S3FileProvider() {
    super();

    setFileNameParser( new S3FileNameParser() );
  }

  protected FileSystem doCreateFileSystem( FileName fileName, FileSystemOptions fileSystemOptions )
    throws FileSystemException {

    S3FileSystemConfigBuilder configBuilder = S3FileSystemConfigBuilder.getInstance();
    S3EncryptionMethod s3EncryptionMethod = configBuilder.getEncryptionMethod( fileSystemOptions ).get();

    // key/secret provided through options?
    AWSCredentialsProvider awsCredentialsProvider;
    if ( configBuilder.getAccessKeyId( fileSystemOptions ).isPresent() ) {
      String accessKey = configBuilder.getAccessKeyId( fileSystemOptions ).get();
      String secretKey = configBuilder.getSecretAccessKey( fileSystemOptions ).get();

      AWSCredentials cred = new BasicAWSCredentials( accessKey, secretKey );
      awsCredentialsProvider = new AWSStaticCredentialsProvider( cred );
    } else {
      // use the default SDK chain resolver
      awsCredentialsProvider = new DefaultAWSCredentialsProviderChain();
    }

    AmazonS3 s3client;
    AmazonS3Builder builder;
    Regions region = configBuilder.getRegion( fileSystemOptions ).get();

    // specific settings for the client
    switch ( s3EncryptionMethod ) {
      case NONE:
      case SERVER_SIDE:
        builder = AmazonS3ClientBuilder
          .standard()
          .withRegion( region );
        break;
      case CLIENT_SIDE:
        builder = AmazonS3EncryptionClientBuilder
          .standard()
          .withRegion( region )
          .withKmsClient( AWSKMSClientBuilder.standard().withRegion( region ).build() )
          .withCryptoConfiguration( new CryptoConfiguration( CryptoMode.AuthenticatedEncryption ) )
          .withEncryptionMaterials(
            new KMSEncryptionMaterialsProvider( configBuilder.getKmsKeyAlias( fileSystemOptions ).get() ) );
        break;

      default:
        builder = AmazonS3ClientBuilder
          .standard();
        break;
    }

    // global settings for the client
    builder.withCredentials( awsCredentialsProvider );
    builder.withForceGlobalBucketAccessEnabled( true );

    s3client = (AmazonS3) builder.build();

    return new S3FileSystem( fileName, fileSystemOptions, s3client );
  }

  public Collection<Capability> getCapabilities() {
    return capabilities;
  }

  @Override public FileSystemConfigBuilder getConfigBuilder() {
    return S3FileSystemConfigBuilder.getInstance();
  }
}
