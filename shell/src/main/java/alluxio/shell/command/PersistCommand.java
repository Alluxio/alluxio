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

package alluxio.shell.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Joiner;

import org.apache.commons.cli.CommandLine;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.Configuration;
import alluxio.exception.AlluxioException;

/**
 * Persists a file or directory currently stored only in Alluxio to the UnderFileSystem
 */
@ThreadSafe
public final class PersistCommand extends AbstractShellCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public PersistCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
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
  public void run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    AlluxioURI inputPath = new AlluxioURI(args[0]);
    persist(inputPath);
  }

  /**
   * Persists a file or directory currently stored only in Alluxio to the UnderFileSystem.
   *
   * @param filePath the {@link AlluxioURI} path to persist to the UnderFileSystem
   * @throws IOException when an Alluxio or I/O error occurs
   */
  private void persist(AlluxioURI filePath) throws IOException {
    try {
      URIStatus status = mFileSystem.getStatus(filePath);
      if (status.isFolder()) {
        List<URIStatus> statuses = mFileSystem.listStatus(filePath);
        List<String> errorMessages = new ArrayList<String>();
        for (URIStatus uriStatus : statuses) {
          AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
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
        long size = FileSystemUtils.persistFile(mFileSystem, filePath, status, mConfiguration);
        System.out.println("persisted file " + filePath + " with size " + size);
      }
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "persist <alluxioPath>";
  }

  @Override
  public String getDescription() {
    return "Persists a file or directory currently stored only in Alluxio to the UnderFileSystem.";
  }
}
