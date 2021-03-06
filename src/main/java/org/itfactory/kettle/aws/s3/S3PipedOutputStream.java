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

import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.EncryptedInitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.KMSEncryptionMaterialsProvider;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.SSEAlgorithm;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.UploadPartRequest;
import org.apache.commons.vfs2.FileSystemOptions;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Custom OutputStream that enables chunked uploads into S3
 *
 * @author asimoes
 * @since 09-11-2017
 */
public class S3PipedOutputStream extends PipedOutputStream {
  private static int PART_SIZE = 1024 * 1024 * 5;
  private ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool( 1 );
  private boolean initialized = false;
  private boolean blockedUntilDone = true;
  private PipedInputStream pipedInputStream;
  private S3AsyncTransferRunner s3AsyncTransferRunner;
  private S3FileSystem fileSystem;
  private Future<Boolean> result = null;
  private String bucketId;
  private String key;

  public S3PipedOutputStream( S3FileSystem fileSystem, String bucketId, String key ) throws IOException {
    this.pipedInputStream = new PipedInputStream();

    try {
      this.pipedInputStream.connect( this );
    } catch ( IOException e ) {
      // FATAL, unexpected
      throw new RuntimeException( e );
    }

    this.s3AsyncTransferRunner = new S3AsyncTransferRunner();
    this.bucketId = bucketId;
    this.key = key;
    this.fileSystem = fileSystem;
  }

  private void initializeWrite() {
    if ( !initialized ) {
      initialized = true;
      result = this.executor.submit( s3AsyncTransferRunner );
    }
  }

  public boolean isBlockedUntilDone() {
    return blockedUntilDone;
  }

  public void setBlockedUntilDone( boolean blockedUntilDone ) {
    this.blockedUntilDone = blockedUntilDone;
  }

  @Override
  public void write( int b ) throws IOException {
    initializeWrite();
    super.write( b );
  }

  @Override
  public void write( byte b[], int off, int len ) throws IOException {
    initializeWrite();
    super.write( b, off, len );
  }

  @Override
  public void close() throws IOException {
    super.close();

    if ( initialized ) {
      if ( isBlockedUntilDone() ) {
        while ( !result.isDone() ) {
          try {
            Thread.sleep( 100 );
          } catch ( InterruptedException e ) {
            e.printStackTrace();
          }
        }
      }
    }

    this.executor.shutdown();
  }

  class S3AsyncTransferRunner implements Callable<Boolean> {

    public Boolean call() throws Exception {
      boolean result = true;
      List<PartETag> partETags = new ArrayList<PartETag>();

      // Step 1: Initialize
      FileSystemOptions fsOpts = fileSystem.getFileSystemOptions();
      S3FileSystemConfigBuilder configBuilder = S3FileSystemConfigBuilder.getInstance();

      // Step 1.1: Initialize encryption settings if required
      S3EncryptionMethod s3EncryptionMethod = configBuilder.getEncryptionMethod( fsOpts ).get();
      Optional<String> kmsKey = configBuilder.getKmsKeyAlias( fsOpts );

      InitiateMultipartUploadRequest initRequest;
      ObjectMetadata objectMetadata;
      switch ( s3EncryptionMethod ) {
        case NONE:
          initRequest = new InitiateMultipartUploadRequest( bucketId, key );
          break;

        case SERVER_SIDE:
          initRequest = new InitiateMultipartUploadRequest( bucketId, key );

          // enable server side encryption
          objectMetadata = new ObjectMetadata();
          if ( kmsKey.isPresent() ) {
            // if user specified a key, use that one
            objectMetadata.setSSEAlgorithm( SSEAlgorithm.KMS.getAlgorithm() );
            initRequest.setSSEAwsKeyManagementParams( new SSEAwsKeyManagementParams( kmsKey.get() ) );
          } else {
            // use an S3 managed key
            objectMetadata.setSSEAlgorithm( SSEAlgorithm.AES256.getAlgorithm() );
          }

          initRequest.setObjectMetadata( objectMetadata );
          break;

        case CLIENT_SIDE:
          initRequest = new EncryptedInitiateMultipartUploadRequest( bucketId, key );

          // introduce key to be used for encryption
          KMSEncryptionMaterialsProvider kmsEncryptionMaterialsProvider =
            new KMSEncryptionMaterialsProvider( kmsKey.get() );
          ( (EncryptedInitiateMultipartUploadRequest) initRequest ).withMaterialsDescription(
            kmsEncryptionMaterialsProvider.getEncryptionMaterials().getMaterialsDescription() );
          break;

        default:
          initRequest = new InitiateMultipartUploadRequest( bucketId, key );
          break;

      }

      InitiateMultipartUploadResult initResponse = fileSystem.getS3().initiateMultipartUpload( initRequest );
      ByteArrayOutputStream baos = new ByteArrayOutputStream( PART_SIZE );

      try {
        // Step 2: Upload parts.
        byte[] tmpBuffer = new byte[ PART_SIZE ];
        int read, offset = 0;
        int totalRead = 0;
        int partNum = 1;
        BufferedInputStream bis = new BufferedInputStream( pipedInputStream, PART_SIZE );
        S3WindowedSubstream s3is;

        while ( ( read = bis.read( tmpBuffer ) ) >= 0 ) {

          // if something was actually read
          if ( read > 0 ) {
            baos.write( tmpBuffer, 0, read );
            totalRead += read;
          }

          if ( totalRead > PART_SIZE ) { // do we have a minimally accepted chunk above 5Mb?
            s3is = new S3WindowedSubstream( baos.toByteArray() );

            UploadPartRequest uploadRequest = new UploadPartRequest()
              .withBucketName( bucketId ).withKey( key )
              .withUploadId( initResponse.getUploadId() ).withPartNumber( partNum++ )
              .withFileOffset( offset )
              .withPartSize( totalRead )
              .withInputStream( s3is );

            // Upload part and add response to our list.
            partETags.add( fileSystem.getS3().uploadPart( uploadRequest ).getPartETag() );

            offset += totalRead;
            totalRead = 0; // reset part size counter
            baos.reset(); // reset output stream to 0
          }
        }

        // Step 2.1 upload last part
        s3is = new S3WindowedSubstream( baos.toByteArray() );

        UploadPartRequest uploadRequest = new UploadPartRequest()
          .withBucketName( bucketId ).withKey( key )
          .withUploadId( initResponse.getUploadId() ).withPartNumber( partNum++ )
          .withFileOffset( offset )
          .withPartSize( totalRead )
          .withInputStream( s3is )
          .withLastPart( true );

        partETags.add( fileSystem.getS3().uploadPart( uploadRequest ).getPartETag() );

        // Step 3: Complete.
        CompleteMultipartUploadRequest compRequest =
          new CompleteMultipartUploadRequest( bucketId, key, initResponse.getUploadId(), partETags );

        fileSystem.getS3().completeMultipartUpload( compRequest );
      } catch ( Exception e ) {
        e.printStackTrace();
        fileSystem.getS3()
          .abortMultipartUpload( new AbortMultipartUploadRequest( bucketId, key, initResponse.getUploadId() ) );
        result = false;
      } finally {
        baos.close();
      }

      return result;
    }
  }
}
