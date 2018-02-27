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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.provider.AbstractFileName;
import org.apache.commons.vfs2.provider.AbstractFileObject;
import org.apache.commons.vfs2.provider.AbstractFileSystem;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * VFS FileObject implementation for S3
 *
 * @author asimoes
 * @since 09-11-2017
 */
public class S3FileObject extends AbstractFileObject {

  public static final String DELIMITER = "/";

  private S3FileSystem fileSystem;
  private S3Object s3Object;
  private String bucketName;
  private String key;

  protected S3FileObject( AbstractFileName name, AbstractFileSystem fs ) {
    super( name, fs );

    this.fileSystem = (S3FileSystem) fs;
    this.bucketName = ( (S3FileName) getName() ).getBucketId();
    this.key = ( (S3FileName) getName() ).getBucketRelativePath();
  }

  private S3Object getS3Object() {
    return getS3Object( this.key );
  }

  private S3Object getS3Object( String key ) {
    return fileSystem.getS3().getObject( bucketName, key );
  }

  private ObjectListing getS3ObjectChildrenHierarchically() {
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
      .withBucketName( bucketName )
      .withPrefix( key )
      .withDelimiter( DELIMITER );

    return fileSystem.getS3().listObjects( listObjectsRequest );
  }

  private ObjectListing getS3ObjectChildren() {
    return fileSystem.getS3().listObjects( bucketName, key );
  }

  private boolean isRootBucket() {
    return key.equals( "" );
  }

  @Override protected void doAttach() throws Exception {
    injectType( FileType.IMAGINARY );

    if ( isRootBucket() ) {
      // cannot attach to root bucket
      injectType( FileType.FOLDER );
      return;
    }

    // 1. Is it an existing file?
    try {
      s3Object = getS3Object();
      injectType( getName().getType() ); // if this worked then the automatically detected type is right
    } catch ( AmazonS3Exception e ) {
      // S3 object doesn't exist

      // 2. Is it in reality a folder?
      String keyWithDelimiter = key + DELIMITER;
      try {
        s3Object = getS3Object( keyWithDelimiter );
        injectType( FileType.FOLDER );
        this.key = keyWithDelimiter;
      } catch ( AmazonS3Exception e2 ) {
        //TODO: there must be a better way to do this...
        String errorCode = e2.getErrorCode();

        // confirms key doesn't exist but connection okay
        if ( errorCode.equals( "NoSuchKey" ) ) {
          // move on
        } else {
          // bubbling up other connection errors
          e2.printStackTrace(); // make sure this gets printed for the user
          throw new FileSystemException( "vfs.provider/get-type.error", this.key );
        }
      }
    }
  }

  @Override
  public void doDelete() throws FileSystemException {

    // can only delete folder if empty
    if ( getType() == FileType.FOLDER ) {

      // list all children inside the folder
      ObjectListing ol = getS3ObjectChildren();
      ArrayList<S3ObjectSummary> allSummaries = new ArrayList<S3ObjectSummary>( ol.getObjectSummaries() );

      // get full list
      while ( ol.isTruncated() ) {
        ol = fileSystem.getS3().listNextBatchOfObjects( ol );
        allSummaries.addAll( ol.getObjectSummaries() );
      }

      for ( S3ObjectSummary s3os : allSummaries ) {
        fileSystem.getS3().deleteObject( bucketName, s3os.getKey() );
      }
    }

    fileSystem.getS3().deleteObject( bucketName, key );
  }

  protected long doGetContentSize() throws Exception {
    return s3Object.getObjectMetadata().getContentLength();
  }

  protected InputStream doGetInputStream() throws Exception {
    return s3Object.getObjectContent();
  }

  @Override
  protected OutputStream doGetOutputStream( boolean bAppend ) throws Exception {
    return new S3PipedOutputStream( this.fileSystem, bucketName, key );
  }

  protected FileType doGetType() throws Exception {
    return getType();
  }

  protected String[] doListChildren() throws Exception {
    List<String> childrenList = new ArrayList<String>();

    // only listing folders or the root bucket
    if ( getType() == FileType.FOLDER || isRootBucket() ) {

      // fix cases where the path doesn't include the final delimiter
      String realKey = key;
      if ( !realKey.endsWith( DELIMITER ) ) {
        realKey += DELIMITER;
      }

      ObjectListing ol = getS3ObjectChildrenHierarchically();
      ArrayList<S3ObjectSummary> allSummaries = new ArrayList<S3ObjectSummary>( ol.getObjectSummaries() );
      ArrayList<String> allCommonPrefixes = new ArrayList<String>( ol.getCommonPrefixes() );

      // get full list
      while ( ol.isTruncated() ) {
        ol = fileSystem.getS3().listNextBatchOfObjects( ol );
        allSummaries.addAll( ol.getObjectSummaries() );
        allCommonPrefixes.addAll( ol.getCommonPrefixes() );
      }

      for ( S3ObjectSummary s3os : ol.getObjectSummaries() ) {
        if ( !s3os.getKey().equals( realKey ) ) {
          childrenList.add( s3os.getKey().substring( key.length() ) );
        }
      }

      for ( String commonPrefix : ol.getCommonPrefixes() ) {
        if ( !commonPrefix.equals( realKey ) ) {
          childrenList.add( commonPrefix.substring( key.length() ) );
        }
      }
    }

    String[] childrenArr = new String[ childrenList.size() ];

    return childrenList.toArray( childrenArr );
  }

  @Override protected long doGetLastModifiedTime() throws Exception {
    return s3Object.getObjectMetadata().getLastModified().getTime();
  }

  @Override protected void doCreateFolder() throws Exception {
    if ( !isRootBucket() ) {
      // create meta-data for your folder and set content-length to 0
      ObjectMetadata metadata = new ObjectMetadata();
      metadata.setContentLength( 0 );
      metadata.setContentType( "binary/octet-stream" );

      // create empty content
      InputStream emptyContent = new ByteArrayInputStream( new byte[ 0 ] );

      // create a PutObjectRequest passing the folder name suffixed by /
      PutObjectRequest putObjectRequest = new PutObjectRequest( bucketName, key + DELIMITER, emptyContent, metadata );

      // send request to S3 to create folder
      try {
        fileSystem.getS3().putObject( putObjectRequest );
      } catch ( AmazonS3Exception e ) {
        throw new FileSystemException( "vfs.provider.local/create-folder.error", this, e );
      }
    } else {
      throw new FileSystemException( "vfs.provider/create-folder-not-supported.error" );
    }
  }

  @Override protected void doRename( FileObject newFile ) throws Exception {

    // no folder renames on S3
    if ( getType().equals( FileType.FOLDER ) ) {
      throw new FileSystemException( "vfs.provider/rename-not-supported.error" );
    }

    if ( s3Object == null ) {
      // object doesn't exist
      throw new FileSystemException( "vfs.provider/rename.error", new Object[] { this, newFile } );
    }

    S3FileObject dest = (S3FileObject) newFile;

    // 1. copy the file
    CopyObjectRequest copyObjRequest = new CopyObjectRequest( bucketName, key, dest.bucketName, dest.key );
    fileSystem.getS3().copyObject( copyObjRequest );

    // 2. delete self
    delete();
  }
}
