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
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.io.Closer;

import tachyon.Constants;
import tachyon.TachyonURI;
import tachyon.client.ReadType;
import tachyon.client.file.FileInStream;
import tachyon.client.file.FileSystem;
import tachyon.client.file.URIStatus;
import tachyon.client.file.options.OpenFileOptions;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

/**
 * Loads a file or directory in Tachyon space, makes it resident in memory.
 */
@ThreadSafe
public final class LoadCommand extends WithWildCardPathCommand {

  /**
   * Constructs a new instance to load a file or directory in Tachyon space.
   *
   * @param conf the configuration for Tachyon
   * @param fs the filesystem of Tachyon
   */
  public LoadCommand(TachyonConf conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "load";
  }

  @Override
  void runCommand(TachyonURI path) throws IOException {
    load(path);
  }

  /**
   * Loads a file or directory in Tachyon space, makes it resident in memory.
   *
   * @param filePath The {@link TachyonURI} path to load into Tachyon memory
   * @throws IOException if a non-Tachyon related exception occurs
   */
  private void load(TachyonURI filePath) throws IOException {
    try {
      URIStatus status = mFileSystem.getStatus(filePath);
      if (status.isFolder()) {
        List<URIStatus> statuses = mFileSystem.listStatus(filePath);
        for (URIStatus uriStatus : statuses) {
          TachyonURI newPath = new TachyonURI(uriStatus.getPath());
          load(newPath);
        }
      } else {
        if (status.getInMemoryPercentage() == 100) {
          // The file has already been fully loaded into Tachyon memory.
          return;
        }
        Closer closer = Closer.create();
        try {
          OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.CACHE_PROMOTE);
          FileInStream in = closer.register(mFileSystem.openFile(filePath, options));
          byte[] buf = new byte[8 * Constants.MB];
          while (in.read(buf) != -1) {
          }
        } catch (Exception e) {
          throw closer.rethrow(e);
        } finally {
          closer.close();
        }
      }
      System.out.println(filePath + " loaded");
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "load <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Tachyon space, makes it resident in memory.";
  }
}
