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

package alluxio.cli.fs.command;

import alluxio.AlluxioURI;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemUtils;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import com.google.common.base.Joiner;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Persists files or directories currently stored only in Alluxio to the UnderFileSystem.
 */
@ThreadSafe
public final class PersistCommand extends AbstractFileSystemCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public PersistCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "persist";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }

  @Override
  protected void runPlainPath(AlluxioURI plainPath, CommandLine cl)
      throws AlluxioException, IOException {
    persist(plainPath);
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    for (String path : args) {
      AlluxioURI inputPath = new AlluxioURI(path);
      runWildCardCmd(inputPath, cl);
    }
    return 0;
  }

  /**
   * Persists a file or directory currently stored only in Alluxio to the UnderFileSystem.
   *
   * @param filePath the {@link AlluxioURI} path to persist to the UnderFileSystem
   */
  private void persist(AlluxioURI filePath) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      List<String> errorMessages = new ArrayList<>();
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        try {
          persist(newPath);
        } catch (Exception e) {
          errorMessages.add(e.getMessage());
        }
      }
      if (errorMessages.size() != 0) {
        throw new IOException(Joiner.on('\n').join(errorMessages));
      }
    } else if (status.isPersisted()) {
      System.out.println(filePath + " is already persisted");
    } else {
      FileSystemUtils.persistFile(mFileSystem, filePath);
      System.out.println("persisted file " + filePath + " with size " + status.getLength());
    }
  }

  @Override
  public String getUsage() {
    return "persist <path> [<path> ...]";
  }

  @Override
  public String getDescription() {
    return "Persists files or directories currently stored only in Alluxio to the "
        + "UnderFileSystem.";
  }
}
