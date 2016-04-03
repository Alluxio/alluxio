/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the “License”). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.shell.command;

import alluxio.AlluxioURI;
import alluxio.Configuration;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the file's contents to the console.
 */
@ThreadSafe
public final class CatCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public CatCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "cat";
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws IOException {
    try {
      URIStatus status = mFileSystem.getStatus(path);

      if (!status.isFolder()) {
        OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
        FileInStream is = mFileSystem.openFile(path, options);
        byte[] buf = new byte[512];
        try {
          int read = is.read(buf);
          while (read != -1) {
            System.out.write(buf, 0, read);
            read = is.read(buf);
          }
        } finally {
          is.close();
        }
      } else {
        throw new IOException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
      }
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }
  }

  @Override
  public String getUsage() {
    return "cat <path>";
  }

  @Override
  public String getDescription() {
    return "Prints the file's contents to the console.";
  }
}
