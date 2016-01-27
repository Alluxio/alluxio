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

package tachyon.client;

import com.google.common.collect.Lists;
import tachyon.TachyonURI;
import tachyon.client.file.FileOutStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.FileSystemContext;
import tachyon.client.file.FileSystemMasterClient;
import tachyon.client.file.options.CreateFileOptions;
import tachyon.thrift.BlockLocation;
import tachyon.thrift.FileBlockInfo;
import tachyon.thrift.WorkerNetAddress;

import java.io.IOException;
import java.util.List;

/**
 * Represents a Tachyon File, legacy API.
 */
public class TachyonFile {
  private final FileSystem mFileSystem;
  private final TachyonURI mPath;

  public TachyonFile(TachyonURI path, FileSystem fs) {
    mFileSystem = fs;
    mPath = path;
  }

  public FileOutStream getOutStream(WriteType writeType) throws IOException {
    try {
      return mFileSystem.createFile(mPath, CreateFileOptions.defaults().setWriteType(writeType));
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

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
}
