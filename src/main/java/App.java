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

import com.amazonaws.regions.Regions;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;
import org.itfactory.kettle.aws.s3.S3EncryptionMethod;
import org.itfactory.kettle.aws.s3.S3FileSystemConfigBuilder;

import java.io.IOException;

public class App {
  public static void main( String[] args ) {
    try {
      FileSystemManager fsManager = VFS.getManager();

      FileSystemOptions fsOpts = new FileSystemOptions();
      S3FileSystemConfigBuilder configBuilder = S3FileSystemConfigBuilder.getInstance();
      configBuilder.setEncryptionMethod( fsOpts, S3EncryptionMethod.SERVER_SIDE );
      configBuilder.setKmsKeyAlias( fsOpts, "alias/test-key" );
      configBuilder.setRegion( fsOpts, Regions.EU_WEST_2 );

      FileObject fileObject;

      /*fileObject = fsManager.resolveFile( "s3sdk://zyy/accessKeys.csv" , fsOpts);

      System.out.println( fileObject.exists() );

      fileObject = fsManager.resolveFile( "s3sdk://itxpander-bucket-1/test-folder1" );

      for (FileObject f : fileObject.getChildren()) {
        System.out.println(f.toString() + " type: " + f.getType());
      }

      fileObject = fsManager.resolveFile( "s3sdk://itxpander-bucket-1/test-folder2/" );

      fileObject.createFolder();
      fileObject.delete();

      */
      fileObject =
        fsManager.resolveFile( "s3sdk://itxpander-bucket-1/test-folder1/XPBIU-HSBCNDAFAQ-240118-1603-84.pdf", fsOpts );

      FileObject orig = fsManager.resolveFile( "file:///home/puls3/Downloads/XPBIU-HSBCNDAFAQ-240118-1603-84.pdf" );

      fileObject.copyFrom( orig, Selectors.SELECT_SELF );

      fileObject.close();

      //FileObject decr = fsManager.resolveFile( "file:///home/puls3/Downloads/XPBIU-HSBCNDAFAQ-240118-1603-84-dec
      // .pdf" );

      //decr.copyFrom( fileObject, Selectors.SELECT_SELF );

      //decr.close();

      /*FileObject newFile = fsManager.resolveFile( "s3sdk://itxpander-bucket-1/test-folder1/sales_data.csv" );

      fileObject.moveTo( newFile );*/

    } catch ( FileSystemException e ) {
      e.printStackTrace();
    } catch ( IOException e ) {
      e.printStackTrace();
    }
  }
}