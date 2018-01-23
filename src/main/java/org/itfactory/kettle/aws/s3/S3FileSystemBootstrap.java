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

import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.pentaho.di.core.annotations.KettleLifecyclePlugin;
import org.pentaho.di.core.lifecycle.KettleLifecycleListener;
import org.pentaho.di.core.lifecycle.LifecycleException;
import org.pentaho.di.core.logging.LogChannel;
import org.pentaho.di.core.logging.LogChannelInterface;
import org.pentaho.di.core.vfs.KettleVFS;
import org.pentaho.di.i18n.BaseMessages;

import java.util.Arrays;

/**
 * Kettle plugin to register the vfs extension for S3
 *
 * @author asimoes
 * @since 09-11-2017
 */
@KettleLifecyclePlugin( id = "S3FileSystemBootstrap", name = "S3 FileSystem Bootstrap" )
public class S3FileSystemBootstrap implements KettleLifecycleListener {
  private static Class<?> PKG = S3FileSystemBootstrap.class;
  private LogChannelInterface log = new LogChannel( S3FileSystemBootstrap.class.getName() );

  public void onEnvironmentInit() throws LifecycleException {
    try {
      // Register S3 as a file system type with VFS
      FileSystemManager fsm = KettleVFS.getInstance().getFileSystemManager();
      if ( fsm instanceof DefaultFileSystemManager ) {
        if ( !Arrays.asList( fsm.getSchemes() ).contains( S3FileProvider.SCHEME ) ) {
          ( (DefaultFileSystemManager) fsm ).addProvider( S3FileProvider.SCHEME, new S3FileProvider() );
        }
      }
    } catch ( FileSystemException e ) {
      log.logError( BaseMessages.getString( PKG, "GCSSpoonPlugin.StartupError.FailedToLoadGCSDriver" ) );
    }
  }

  public void onEnvironmentShutdown() {

  }
}
