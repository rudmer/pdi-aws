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

import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.Selectors;
import org.apache.commons.vfs2.VFS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;

public class App {
  public static void main( String[] args ) {
    try {
      FileSystemManager fsManager = VFS.getManager();

      FileObject fileObject = fsManager.resolveFile( "s3sdk://cn-north-1:zyy/accessKeys.csv" );

      System.out.println( fileObject.exists() );

      /*fileObject = fsManager.resolveFile( "s3sdk://itxpander-bucket-1/test-folder1" );

      for (FileObject f : fileObject.getChildren()) {
        System.out.println(f.toString() + " type: " + f.getType());
      }

      fileObject = fsManager.resolveFile( "s3sdk://itxpander-bucket-1/test-folder2/" );

      fileObject.createFolder();
      fileObject.delete();

      fileObject = fsManager.resolveFile( "s3sdk://itxpander-bucket-1/test-folder1/jd-gui-1.4.0_vfs.jar" );

      FileObject orig = fsManager.resolveFile( "file:///Users/puls3/Downloads/jd-gui-1.4.0.jar" );

      fileObject.copyFrom( orig, Selectors.SELECT_SELF );

      fileObject.close();

      FileObject newFile = fsManager.resolveFile( "s3sdk://itxpander-bucket-1/test-folder1/sales_data.csv" );

      fileObject.moveTo( newFile );*/

    } catch ( FileSystemException e ) {
      e.printStackTrace();
    } catch ( IOException e ) {
      e.printStackTrace();
    }
  }
}