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

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import java.util.Collection;

/**
 * VFS FileSystem implementation for S3
 *
 * @author asimoes
 * @since 09-11-2017
 */
public class S3FileSystem extends AbstractFileSystem implements FileSystem {

  private AmazonS3 s3;

  protected S3FileSystem( FileName rootName, FileSystemOptions fileSystemOptions ) {
    super( rootName, null, fileSystemOptions );
  }

  protected FileObject createFile( AbstractFileName abstractFileName ) throws Exception {
    return new S3FileObject( abstractFileName, this );
  }

  protected void addCapabilities( Collection<Capability> collection ) {
    collection.addAll( S3FileProvider.capabilities );
  }

  @Override protected void doCloseCommunicationLink() {
    s3.shutdown();
  }

  public AmazonS3 getS3() {
    if ( s3 == null ) {
      s3 = AmazonS3ClientBuilder
        .standard()
        .withForceGlobalBucketAccessEnabled( true )
        .withRegion( Regions.fromName( ((S3FileName)this.getRootName()).getRegionId() ) )
        .build();
    }

    return s3;
  }
}
