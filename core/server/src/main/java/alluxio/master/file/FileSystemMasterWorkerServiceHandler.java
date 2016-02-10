/*
 * Licensed to the University of California, Berkeley under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package alluxio.master.file;

import alluxio.Constants;
import alluxio.exception.AccessControlException;
import alluxio.exception.AlluxioException;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.InvalidPathException;
import alluxio.thrift.AlluxioTException;
import alluxio.thrift.FileInfo;
import alluxio.thrift.FileSystemCommand;
import alluxio.thrift.FileSystemMasterWorkerService;
import alluxio.wire.ThriftUtils;

import com.google.common.base.Preconditions;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Set;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * This class is a Thrift handler for file system master RPCs invoked by an Alluxio worker.
 */
@NotThreadSafe // TODO(jiri): make thread-safe (c.f. ALLUXIO-1664)
public final class FileSystemMasterWorkerServiceHandler
    implements FileSystemMasterWorkerService.Iface {
  private final FileSystemMaster mFileSystemMaster;

  /**
   * Creates a new instance of {@link FileSystemMasterWorkerServiceHandler}.
   *
   * @param fileSystemMaster the {@link FileSystemMaster} the handler uses internally
   */
  public FileSystemMasterWorkerServiceHandler(FileSystemMaster fileSystemMaster) {
    Preconditions.checkNotNull(fileSystemMaster);
    mFileSystemMaster = fileSystemMaster;
  }

  @Override
  public long getServiceVersion() {
    return Constants.FILE_SYSTEM_MASTER_WORKER_SERVICE_VERSION;
  }

  @Override
  public FileInfo getFileInfo(long fileId) throws AlluxioTException {
    try {
      return ThriftUtils.toThrift(mFileSystemMaster.getFileInfo(fileId));
    } catch (AlluxioException e) {
      throw e.toAlluxioTException();
    }
  }

  @Override
  public Set<Long> getPinIdList() {
    return mFileSystemMaster.getPinIdList();
  }

  @Override
  public FileSystemCommand heartbeat(long workerId, List<Long> persistedFiles)
      throws AlluxioTException, TException {
    try {
      return mFileSystemMaster.workerHeartbeat(workerId, persistedFiles);
    } catch (FileDoesNotExistException e) {
      throw e.toAlluxioTException();
    } catch (InvalidPathException e) {
      throw e.toAlluxioTException();
    } catch (AccessControlException e) {
      throw e.toAlluxioTException();
    }
  }
}
