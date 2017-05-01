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
import alluxio.exception.ExceptionMessage;

import com.google.common.base.Preconditions;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the file's last n bytes (by default, 1KB) to the console.
 */
@ThreadSafe
public final class TailCommand extends WithWildCardPathCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public TailCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "tail";
  }

  @Override
  protected void runCommand(AlluxioURI path, CommandLine cl) throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);

    int numOfBytes = Constants.KB;
    if (cl.hasOption('c')) {
      numOfBytes = Integer.parseInt(cl.getOptionValue('c'));
      Preconditions.checkArgument(numOfBytes > 0, "specified bytes must be > 0");
    }

    if (status.isFolder()) {
      throw new IOException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }
    OpenFileOptions options = OpenFileOptions.defaults().setReadType(ReadType.NO_CACHE);
    try (FileInStream is = mFileSystem.openFile(path, options)) {
      byte[] buf = new byte[numOfBytes];
      long bytesToRead;
      if (status.getLength() > numOfBytes) {
        bytesToRead = numOfBytes;
      } else {
        bytesToRead = status.getLength();
      }
      is.skip(status.getLength() - bytesToRead);
      int read = is.read(buf);
      if (read != -1) {
        System.out.write(buf, 0, read);
      }
    }
  }

  @Override
  public String getUsage() {
    return "tail -c <number of bytes> <path>";
  }

  @Override
  public String getDescription() {
    return "Prints the file's last n bytes (by default, 1KB) to the console.";
  }

  @Override
  public Options getOptions() {
    Option bytesOption =
        Option.builder("c").required(false).numberOfArgs(1).desc("user specified option").build();
    return new Options().addOption(bytesOption);
  }
}
