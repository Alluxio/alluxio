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
import alluxio.Constants;
import alluxio.annotation.PublicApi;
import alluxio.cli.CommandUtils;
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.util.FormatUtils;

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
@PublicApi
public final class TailCommand extends AbstractFileSystemCommand {
  private static final Option BYTES_OPTION = Option.builder("c")
      .required(false)
      .numberOfArgs(1)
      .desc("number of bytes (e.g., 1024, 4KB)")
      .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public TailCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "tail";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);
    int numOfBytes = Constants.KB;
    if (cl.hasOption('c')) {
      numOfBytes = (int) FormatUtils.parseSpaceSize(cl.getOptionValue('c'));
      Preconditions.checkArgument(numOfBytes > 0, "specified bytes must be > 0");
    }

    if (status.isFolder()) {
      throw new IOException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }
    try (FileInStream is = mFileSystem.openFile(path)) {
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
  public int run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    runWildCardCmd(path, cl);
    return 0;
  }

  @Override
  public String getUsage() {
    return "tail [-c <bytes>] <path>";
  }

  @Override
  public String getDescription() {
    return "Prints the file's last n bytes (by default, 1KB) to the console.";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public Options getOptions() {
    return new Options().addOption(BYTES_OPTION);
  }
}
