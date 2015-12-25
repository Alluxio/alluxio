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

package tachyon.shell.command;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import tachyon.TachyonURI;
import tachyon.client.file.TachyonFile;
import tachyon.client.file.TachyonFileSystem;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;
import tachyon.thrift.FileInfo;
import tachyon.util.FormatUtils;

/**
 * Parent class for commands ls and lsr.
 */
public abstract class AbstractLsCommand extends WithWildCardPathCommand {

  protected AbstractLsCommand(TachyonConf conf, TachyonFileSystem tfs) {
    super(conf, tfs);
  }

  /**
   * Displays information for all directories and files directly under the path specified in args.
   *
   * @param path The {@link TachyonURI} path as the input of the command
   * @param recursive Whether list the path recursively
   * @throws IOException
   */
  protected void ls(TachyonURI path, boolean recursive) throws IOException {
    List<FileInfo> files = listStatusSortedByIncreasingCreationTime(path);
    String format = "%-10s%-25s%-15s%-15s%-15s%-5s%n";
    for (FileInfo file : files) {
      String inMemory = "";
      if (!file.isFolder) {
        if (100 == file.inMemoryPercentage) {
          inMemory = "In Memory";
        } else {
          inMemory = "Not In Memory";
        }
      }
      System.out.format(format, FormatUtils.getSizeFromBytes(file.getLength()),
          CommandUtils.convertMsToDate(file.getCreationTimeMs()), inMemory, file.getUserName(),
          file.getGroupName(), file.getPath());
      if (recursive && file.isFolder) {
        ls(new TachyonURI(path.getScheme(), path.getAuthority(), file.getPath()), true);
      }
    }
  }

  private List<FileInfo> listStatusSortedByIncreasingCreationTime(TachyonURI path)
      throws IOException {
    List<FileInfo> files = null;
    try {
      TachyonFile fd = mTfs.open(path);
      files = mTfs.listStatus(fd);
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
    Collections.sort(files, new Comparator<FileInfo>() {
      @Override
      public int compare(FileInfo fileInfo, FileInfo fileInfo2) {
        long t1 = fileInfo.creationTimeMs;
        long t2 = fileInfo2.creationTimeMs;
        if (t1 < t2) {
          return -1;
        }
        if (t1 == t2) {
          return 0;
        }
        return 1;
      }
    });
    return files;
  }
}
