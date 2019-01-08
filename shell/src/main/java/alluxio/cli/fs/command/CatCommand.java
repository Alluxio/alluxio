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
import alluxio.client.file.FileInStream;
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.ExceptionMessage;
import alluxio.exception.FileDoesNotExistException;
import alluxio.exception.status.InvalidArgumentException;
import alluxio.grpc.OpenFilePOptions;

import org.apache.commons.cli.CommandLine;

import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Prints the file's contents to the console.
 */
@ThreadSafe
public final class CatCommand extends AbstractFileSystemCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public CatCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "cat";
  }

  @Override
  protected void runPlainPath(AlluxioURI path, CommandLine cl)
      throws AlluxioException, IOException {
    URIStatus status = mFileSystem.getStatus(path);

    if (status.isFolder()) {
      throw new FileDoesNotExistException(ExceptionMessage.PATH_MUST_BE_FILE.getMessage(path));
    }
    OpenFilePOptions options = OpenFilePOptions.getDefaultInstance();
    byte[] buf = new byte[512];
    try (FileInStream is = mFileSystem.openFile(path, options)) {
      int read = is.read(buf);
      while (read != -1) {
        System.out.write(buf, 0, read);
        read = is.read(buf);
      }
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

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsNoLessThan(this, cl, 1);
  }

  @Override
  public int run(CommandLine cl) throws IOException {
    String[] args = cl.getArgs();
    runWildCardCmd(new AlluxioURI(args[0]), cl);

    return 0;
  }
}
