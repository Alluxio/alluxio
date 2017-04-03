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
import alluxio.client.file.FileSystem;
import alluxio.client.file.URIStatus;
import alluxio.exception.AlluxioException;
import java.io.IOException;
import net.jcip.annotations.ThreadSafe;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * Tests properties of the path specified in args.
 */
@ThreadSafe
public final class TestCommand extends AbstractShellCommand {

  /**
   * @param fs the filesystem of Alluxio
   */
  public TestCommand(FileSystem fs) {
    super(fs);
  }

  @Override
  public String getCommandName() {
    return "test";
  }

  @Override
  public int getNumOfArgs() {
    return 1;
  }

  @Override
  protected Options getOptions() {
    return new Options()
        .addOption(DIR_OPTION)
        .addOption(FILE_OPTION)
        .addOption(PATH_EXIST_OPTION)
        .addOption(DIR_NOT_EMPTY_OPTION)
        .addOption(FILE_ZERO_LENGTH_OPTION);
  }

  private void printResult(boolean testResult) {
    if (testResult) {
      System.out.printf("%d\n", 0);
    } else {
      System.out.printf("%d\n", 1);
    }
  }

  /**
   * Tests properties of the path specified in args.
   *
   * @param path The {@link AlluxioURI} path as the input of the command
   * @param testDir test whether the path is a directory
   * @param testFile test whether the path is a file
   * @param testPathExist test whether the path exists
   * @param testDirNotEmpty test whether the directory is not empty
   * @param testFileZeroLength test whether the file is zero length
   * @throws AlluxioException when Alluxio exception occurs
   * @throws IOException when non-Alluxio exception occurs
   */
  public void test(AlluxioURI path, boolean testDir, boolean testFile, boolean testPathExist,
                   boolean testDirNotEmpty, boolean testFileZeroLength)
                   throws AlluxioException, IOException {
    try {
      URIStatus status = mFileSystem.getStatus(path);
      boolean testResult = false;
      if (testDir && status.isFolder()) {
        testResult = true;
      } else if (testFile && !status.isFolder()) {
        testResult = true;
      } else if (testPathExist) {
        testResult = true;
      } else if (testDirNotEmpty && status.isFolder() && status.getLength() > 0) {
        testResult = true;
      } else if (testFileZeroLength && !status.isFolder() && status.getLength() == 0) {
        testResult = true;
      }
      printResult(testResult);
    } catch (AlluxioException | IOException e) {
      System.out.printf("%d\n", 1);
    }
  }

  @Override
  public void run(CommandLine cl) throws AlluxioException, IOException {
    String[] args = cl.getArgs();
    AlluxioURI path = new AlluxioURI(args[0]);
    test(path, cl.hasOption("d"), cl.hasOption("f"), cl.hasOption("e"),
         cl.hasOption("s"), cl.hasOption("z"));
  }

  @Override
  public String getUsage() {
    return "test [-d|-f|-e|-s|-z] <path>";
  }

  @Override
  public String getDescription() {
    return "Display properties of the path."
        + " Specify -d to test whether the path is a directory."
        + " Specify -f to test whether the path is a file."
        + " Specify -e to test whether the path exists."
        + " Specify -s to test whether the directory is not empty."
        + " Specify -z to test whether the file is zero length.";
  }
}
