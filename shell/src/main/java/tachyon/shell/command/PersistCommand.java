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
import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;

import tachyon.TachyonURI;
import tachyon.client.file.FileSystem;
import tachyon.client.file.FileSystemUtils;
import tachyon.client.file.URIStatus;
import tachyon.conf.TachyonConf;
import tachyon.exception.TachyonException;

/**
 * Persists a file or directory currently stored only in Tachyon to the UnderFileSystem
 */
public final class PersistCommand extends AbstractTfsShellCommand {

  /**
   * @param conf the configuration for Tachyon
   * @param tfs the filesystem of Tachyon
   */
  public PersistCommand(TachyonConf conf, FileSystem tfs) {
    super(conf, tfs);
  }

  @Override
  public String getCommandName() {
    return "persist";
  }

  @Override
  protected int getNumOfArgs() {
    return 1;
  }

  @Override
  public void run(String... args) throws IOException {
    TachyonURI inputPath = new TachyonURI(args[0]);
    persist(inputPath);
  }

  /**
   * Persists a file or directory currently stored only in Tachyon to the UnderFileSystem.
   *
   * @param filePath the {@link TachyonURI} path to persist to the UnderFileSystem
   * @throws IOException when a Tachyon or I/O error occurs
   */
  private void persist(TachyonURI filePath) throws IOException {
    try {
      URIStatus status = mTfs.getStatus(filePath);
      if (status.isFolder()) {
        List<URIStatus> statuses = mTfs.listStatus(filePath);
        List<String> errorMessages = new ArrayList<String>();
        for (URIStatus uriStatus : statuses) {
          TachyonURI newPath = new TachyonURI(uriStatus.getPath());
          try {
            persist(newPath);
          } catch (IOException e) {
            errorMessages.add(e.getMessage());
          }
        }
        if (errorMessages.size() != 0) {
          throw new IOException(Joiner.on('\n').join(errorMessages));
        }
      } else if (status.isPersisted()) {
        System.out.println(filePath + " is already persisted");
      } else {
        long size = FileSystemUtils.persistFile(mTfs, filePath, status, mTachyonConf);
        System.out.println("persisted file " + filePath + " with size " + size);
      }
    } catch (TachyonException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "persist <tachyonPath>";
  }

  @Override
  public String getDescription() {
    return "Persists a file or directory currently stored only in Tachyon to the UnderFileSystem.";
  }
}
