/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.file;

import javax.annotation.concurrent.NotThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class is a gRPC handler for file system master RPCs invoked by an Alluxio client.
 */
@NotThreadSafe
public final class FileSystemMasterClientServiceHandlerNew {
  private static final Logger LOG =
      LoggerFactory.getLogger(FileSystemMasterClientServiceHandlerNew.class);
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterClientServiceHandlerNew}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  FileSystemMasterClientServiceHandlerNew(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster, "fileSystemMaster");
    mFileSystemMaster = fileSystemMaster;
  }
}
