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
import alluxio.client.file.FileSystemContext;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import alluxio.exception.status.InvalidArgumentException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Tests properties of the path specified in args.
 */
@ThreadSafe
public final class TestCommand extends AbstractFileSystemCommand {

  private static final Option DIR_OPTION =
      Option.builder("d")
          .required(false)
          .hasArg(false)
          .desc("test whether path is a directory.")
          .build();
  private static final Option FILE_OPTION =
      Option.builder("f")
          .required(false)
          .hasArg(false)
          .desc("test whether path is a file.")
          .build();
  private static final Option PATH_EXIST_OPTION =
      Option.builder("e")
          .required(false)
          .hasArg(false)
          .desc("test whether path exists.")
          .build();
  private static final Option DIR_NOT_EMPTY_OPTION =
      Option.builder("s")
          .required(false)
          .hasArg(false)
          .desc("test whether path is not empty.")
          .build();
  private static final Option FILE_ZERO_LENGTH_OPTION =
      Option.builder("z")
          .required(false)
          .hasArg(false)
          .desc("test whether file is zero length.")
          .build();

  /**
   * @param fsContext the filesystem of Alluxio
   */
  public TestCommand(FileSystemContext fsContext) {
    super(fsContext);
  }

  @Override
  public String getCommandName() {
    return "test";
  }

  @Override
  public void validateArgs(CommandLine cl) throws InvalidArgumentException {
    CommandUtils.checkNumOfArgsEquals(this, cl, 1);
  }

  @Override
  public Options getOptions() {
    return new Options()
        .addOption(DIR_OPTION)
        .addOption(FILE_OPTION)
        .addOption(PATH_EXIST_OPTION)
        .addOption(DIR_NOT_EMPTY_OPTION)
        .addOption(FILE_ZERO_LENGTH_OPTION);
  }

  /**
   * Tests whether the path is a directory.
   *
   * @param status the {@link URIStatus} status as the input of the command
   * @return true if the path is a directory or false if it is not a directory
   */
  private boolean isDir(URIStatus status) {
    return status.isFolder();
  }

  /**
   * Tests whether the path is a file.
   *
   * @param status the {@link URIStatus} status as the input of the command
   * @return true if the path is a file or false if it is not a file
   */
  private boolean isFile(URIStatus status) {
    return !status.isFolder();
  }

  /**
   * Tests whether the directory is not empty.
   *
   * @param status the {@link URIStatus} status as the input of the command
   * @return true if the directory is not empty or false if it is empty
   */
  private boolean isNonEmptyDir(URIStatus status) {
    return status.isFolder() && status.getLength() > 0;
  }

  /**
   * Tests whether the file is zero length.
   *
   * @param status the {@link URIStatus} status as the input of the command
   * @return true if the file is zero length or false if it is not zero length
   */
  private boolean isZeroLengthFile(URIStatus status) {
    return !status.isFolder() && status.getLength() == 0;
  }

  @Override
  public int run(CommandLine cl) throws AlluxioException, IOException {
    if (cl.getOptions().length > 1) {
      return -1;
    }
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    try {
      URIStatus status = mFileSystem.getStatus(path);
      boolean testResult = false;
      if (cl.hasOption("d")) {
        if (isDir(status)) {
          testResult = true;
        }
      } else if (cl.hasOption("f")) {
        if (isFile(status)) {
          testResult = true;
        }
      } else if (cl.hasOption("e")) {
        testResult = true;
      } else if (cl.hasOption("s")) {
        if (isNonEmptyDir(status)) {
          testResult = true;
        }
      } else if (cl.hasOption("z")) {
        if (isZeroLengthFile(status)) {
          testResult = true;
        }
      } else {
        return -1;
      }
      return testResult ? 0 : 1;
    } catch (FileNotFoundException e) {
      return 1;
    } catch (IOException e) {
      return 2;
    } catch (AlluxioException e) {
      return 3;
    }
  }

  @Override
  public String getUsage() {
    return "test [-d|-f|-e|-s|-z] <path>";
  }

  @Override
  public String getDescription() {
    return "Test a property of a path, returning 0 if the property is true, or 1 otherwise.";
  }
}
