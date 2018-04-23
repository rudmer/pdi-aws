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
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
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
import java.util.Optional;

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

    AWSCredentialsProvider awsCredentialsProvider;

    // load profile by name?
    if ( configBuilder.getProfile( fileSystemOptions ).isPresent() ) {
      try {
        awsCredentialsProvider = new ProfileCredentialsProvider( configBuilder.getProfile( fileSystemOptions ).get() );
      } catch ( IllegalArgumentException iae ) {
        // cannot find profile
        throw new FileSystemException( iae );
      }
    }

    // key/secret provided through options?
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
    Optional<Regions> region = configBuilder.getRegion( fileSystemOptions );
    Optional<String> endpoint = configBuilder.getEndpoint( fileSystemOptions );
    Optional<String> kmsKeyAlias = configBuilder.getKmsKeyAlias( fileSystemOptions );

    // specific settings for the client
    switch ( s3EncryptionMethod ) {
      case NONE:
      case SERVER_SIDE:
        builder = AmazonS3ClientBuilder
          .standard();
        break;
      case CLIENT_SIDE:
        builder = AmazonS3EncryptionClientBuilder.standard();
        ( (AmazonS3EncryptionClientBuilder) builder )
          .withCryptoConfiguration( new CryptoConfiguration( CryptoMode.AuthenticatedEncryption ) );

        if ( kmsKeyAlias.isPresent() ) {
          ( (AmazonS3EncryptionClientBuilder) builder ).withEncryptionMaterials(
            new KMSEncryptionMaterialsProvider( kmsKeyAlias.get() ) );
        } else {
          throw new FileSystemException( "Cannot use client side encryption without specifying the KMS key alias." );
        }

        AWSKMSClientBuilder awskmsClientBuilder = AWSKMSClientBuilder.standard();
        awskmsClientBuilder.withCredentials( awsCredentialsProvider );

        awskmsClientBuilder.withRegion( region.orElse( Regions.DEFAULT_REGION ) );

        ( (AmazonS3EncryptionClientBuilder) builder ).withKmsClient( awskmsClientBuilder.build() );

        break;

      default:
        builder = AmazonS3ClientBuilder
          .standard();
        break;
    }

    // global settings for the client
    region.ifPresent( builder::withRegion );
    endpoint.ifPresent( e -> builder.withEndpointConfiguration( new AwsClientBuilder.EndpointConfiguration(e, null) ) );

    if ( !region.isPresent() && !endpoint.isPresent() ) {
      builder.withRegion( Regions.DEFAULT_REGION );
    }

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
