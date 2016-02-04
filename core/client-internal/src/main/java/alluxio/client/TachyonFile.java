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

package alluxio.client;

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;

import alluxio.AlluxioURI;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileOutStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.FileSystemMasterClient;
import alluxio.client.file.options.CreateFileOptions;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.wire.BlockLocation;
import alluxio.wire.FileBlockInfo;
import alluxio.wire.WorkerNetAddress;

/**
 * @deprecated {@see FileSystem} for the supported API.
 * Represents a Tachyon File, legacy API.
 */
@Deprecated
public final class TachyonFile {
  private final FileSystem mFileSystem;
  private final AlluxioURI mPath;

  protected TachyonFile(AlluxioURI path, FileSystem fs) {
    mFileSystem = fs;
    mPath = path;
  }

  /**
   * @return a list of hostnames which contain blocks of the file
   * @throws IOException if an error occurs resolving the file
   */
  public List<String> getLocationHosts() throws IOException {
    List<String> locations = Lists.newArrayList();

    FileSystemMasterClient master = FileSystemContext.INSTANCE.acquireMasterClient();
    try {
      List<FileBlockInfo> blockInfos = master.getFileBlockInfoList(mPath);
      // Add Tachyon locations first
      for (FileBlockInfo info : blockInfos) {
        List<BlockLocation> blockLocations = info.getBlockInfo().getLocations();
        for (BlockLocation loc : blockLocations) {
          locations.add(loc.getWorkerAddress().getHost());
        }
      }
      //Add UFS locations
      for (FileBlockInfo info : blockInfos) {
        for (WorkerNetAddress loc : info.getUfsLocations()) {
          locations.add(loc.getHost());
        }
      }
      return locations;
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      FileSystemContext.INSTANCE.releaseMasterClient(master);
    }
  }

  /**
   * @param readType the {@link ReadType} to use
   * @return a {@link FileInStream} which provides a stream interface to read from the file
   * @throws IOException if an error occurs opening the file
   */
  public FileInStream getInStream(ReadType readType) throws IOException {
    try {
      return mFileSystem.openFile(mPath, OpenFileOptions.defaults().setReadType(readType));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * Creates a file and provides a stream interface to write to the file.
   *
   * @param writeType the {@link WriteType} to use
   * @return a {@link FileOutStream} which can be used to write data to a file
   * @throws IOException if an error occurs creating the file
   */
  public FileOutStream getOutStream(WriteType writeType) throws IOException {
    try {
      return mFileSystem.createFile(mPath, CreateFileOptions.defaults().setWriteType(writeType));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  /**
   * @return the path of this file
   */
  public String getPath() {
    return mPath.getPath();
  }

  /**
   * @return the length in bytes of the file
   * @throws IOException if an error occurs resolving the file
   */
  public long length() throws IOException {
    try {
      return mFileSystem.getStatus(mPath).getLength();
    } catch (Exception e) {
      throw new IOException(e);
    }
  }
}
