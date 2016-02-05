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

import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.cli.CommandLine;

import alluxio.AlluxioURI;
import alluxio.client.ReadType;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.client.file.options.OpenFileOptions;
import alluxio.Configuration;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.AlluxioException;

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
