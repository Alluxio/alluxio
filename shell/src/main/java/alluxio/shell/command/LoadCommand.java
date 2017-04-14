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

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Constants;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;

import com.google.common.io.Closer;
import org.apache.commons.cli.CommandLine;

import java.io.IOException;
import java.util.List;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Loads a file or directory in Alluxio space, making it resident in memory.
 */
@ThreadSafe
public final class LoadCommand extends WithWildCardPathCommand {

  /**
   * Constructs a new instance to load a file or directory in Alluxio space.
   *
   * @param fs the filesystem of Alluxio
   */
  public LoadCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "load";
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    load(path);
  }

  /**
   * Loads a file or directory in Alluxio space, makes it resident in memory.
   *
   * @param filePath The {@link AlluxioURI} path to load into Alluxio memory
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  private void load(AlluxioURI filePath) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(filePath);
    if (status.isFolder()) {
      List<URIStatus> statuses = mFileSystem.listStatus(filePath);
      for (URIStatus uriStatus : statuses) {
        AlluxioURI newPath = new AlluxioURI(uriStatus.getPath());
        load(newPath);
      }
    } else {
      if (status.getInMemoryPercentage() == 100) {
        // The file has already been fully loaded into Alluxio memory.
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
  }

  @Override
  public String getUsage() {
    return "load <path>";
  }

  @Override
  public String getDescription() {
    return "Loads a file or directory in Alluxio space, makes it resident in memory.";
  }
}
