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

import com.amazonaws.regions.Regions;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemOptions;

import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static java.util.Optional.empty;
import static java.util.Optional.of;
import static java.util.Optional.ofNullable;

/**
 * S3 Configuration Options
 *
 * @author asimoes
 * @since 25-01-2018
 */
public class S3FileSystemConfigBuilder extends FileSystemConfigBuilder {
  private static final String PROFILE = "profile";
  private static final String ENDPOINT = "endpoint";
  private static final String REGION = "region";
  private static final String ACCESS_KEY_ID = "accessKeyId";
  private static final String SECRET_ACCESS_KEY = "secretAccessKey";
  private static final String ENCRYPTION_METHOD = "encryptionMethod";
  private static final String KMS_KEY_ALIAS = "kms.keyAlias";

  private static final S3FileSystemConfigBuilder BUILDER = new S3FileSystemConfigBuilder();

  private S3FileSystemConfigBuilder() {
    super( S3FileProvider.SCHEME + "." );
  }

  public static S3FileSystemConfigBuilder getInstance() {
    return BUILDER;
  }

  protected Class<? extends FileSystem> getConfigClass() {
    return S3FileSystem.class;
  }

  public Optional<String> getEndpoint( FileSystemOptions opts ) {
    return ofNullable( getString( opts, ENDPOINT ) );
  }

  public void setEndpoint( FileSystemOptions opts, String endpoint ) {
    if ( getRegion( opts ).isPresent() ) {
      throw new IllegalArgumentException( "Cannot set both Region and Endpoint" );
    }

    setParam( opts, ENDPOINT, endpoint );
  }

  public Optional<Regions> getRegion( FileSystemOptions opts ) {
    String r = getString( opts, REGION );

    return r == null ? empty() : of( Regions.fromName( r ) );
  }

  public void setRegion( FileSystemOptions opts, String region ) {
    if ( getEndpoint( opts ).isPresent() ) {
      throw new IllegalArgumentException( "Cannot set both Region and Endpoint" );
    }

    // check if the region is valid, will throw an IAE
    Regions r = Regions.fromName( requireNonNull( region ) );

    setParam( opts, REGION, region );
  }

  public Optional<String> getAccessKeyId( FileSystemOptions opts ) {
    return ofNullable( getString( opts, ACCESS_KEY_ID ) );
  }

  public void setAccessKeyId( FileSystemOptions opts, String accessKeyId ) {
    setParam( opts, ACCESS_KEY_ID, accessKeyId );
  }

  public Optional<String> getProfile( FileSystemOptions opts ) {
    return ofNullable( getString( opts, PROFILE ) );
  }

  public void setProfile( FileSystemOptions opts, String profile ) {
    if ( getAccessKeyId( opts ).isPresent() ) {
      throw new IllegalArgumentException(
        "Cannot set both profile and static credentials (accessKeyId and secretAccessKey)" );
    }

    setParam( opts, PROFILE, profile );
  }

  public Optional<String> getSecretAccessKey( FileSystemOptions opts ) {
    return ofNullable( getString( opts, SECRET_ACCESS_KEY ) );
  }

  public void setSecretAccessKey( FileSystemOptions opts, String secretAccessKey ) {
    setParam( opts, SECRET_ACCESS_KEY, secretAccessKey );
  }

  public Optional<S3EncryptionMethod> getEncryptionMethod( FileSystemOptions opts ) {
    return of( S3EncryptionMethod.valueOf( getString( opts, ENCRYPTION_METHOD, S3EncryptionMethod.NONE.name() ) ) );
  }

  public void setEncryptionMethod( FileSystemOptions opts, S3EncryptionMethod s3EncryptionMethod ) {
    setParam( opts, ENCRYPTION_METHOD, s3EncryptionMethod.name() );
  }

  public Optional<String> getKmsKeyAlias( FileSystemOptions opts ) {
    return ofNullable( getString( opts, KMS_KEY_ALIAS ) );
  }

  public void setKmsKeyAlias( FileSystemOptions opts, String kmsKeyAlias ) {
    setParam( opts, KMS_KEY_ALIAS, kmsKeyAlias );
  }
}
