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
import alluxio.Constants;
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
 * Prints the file's last 1KB of contents to the console.
 */
@ThreadSafe
public final class TailCommand extends WithWildCardPathCommand {

  /**
   * @param conf the configuration for Alluxio
   * @param fs the filesystem of Alluxio
   */
  public TailCommand(Configuration conf, FileSystem fs) {
    super(conf, fs);
  }

  @Override
  public String getCommandName() {
    return "tail";
  }

  @Override
  void runCommand(AlluxioURI path, CommandLine cl) throws IOException {
    URIStatus status;
    try {
      status = mFileSystem.getStatus(path);
    } catch (AlluxioException e) {
      throw new IOException(e.getMessage());
    }

    if (!status.isFolder()) {
      OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
      FileInStream is = null;
      try {
        is = mFileSystem.openFile(path, options);
        byte[] buf = new byte[Constants.KB];
        long bytesToRead = 0L;
        if (status.getLength() > Constants.KB) {
          bytesToRead = Constants.KB;
        } else {
          bytesToRead = status.getLength();
        }
        is.skip(status.getLength() - bytesToRead);
        int read = is.read(buf);
        if (read != -1) {
          System.out.write(buf, 0, read);
        }
      } catch (AlluxioException e) {
        throw new IOException(e.getMessage());
      } finally {
        is.close();
      }
    } else {
      throw new IOException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }
  }

  @Override
  public String getUsage() {
    return "tail <path>";
  }

  @Override
  public String getDescription() {
    return "Prints the file's last 1KB of contents to the console.";
  }
}
