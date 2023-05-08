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

package alluxio.cli;

import alluxio.AlluxioURI;
import alluxio.client.file.FileSystem;
import alluxio.client.file.FileSystemContext;
import alluxio.grpc.DeletePOptions;
import alluxio.util.io.PathUtils;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.internal.Lists;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Driver to Alluxio tests.
 */
@ThreadSafe
public final class AlluxioTests {
  @Parameter(names = "--directory",
      description = "Alluxio path for the tests working directory.")
  private String mDirectory = AlluxioURI.SEPARATOR;

  @Parameter(names = {"-h", "--help"}, description = "Prints usage information", help = true)
  private boolean mHelp;

  private AlluxioTests() {} // prevent instantiation

  /** Directory for the test generated files. */
  public static final String TEST_DIRECTORY_NAME = "alluxioTests_files";

  /**
   * Console program that validates the configuration.
   *
   * @param args there are no arguments needed
   */
  public static void main(String[] args) throws Exception {
    AlluxioTests runner = new AlluxioTests();
    JCommander jCommander = new JCommander(runner);
    jCommander.setProgramName("TestRunner");
    jCommander.parse(args);
    if (runner.mHelp) {
      jCommander.usage();
      return;
    }

    int ret = runner.runAlluxioTests();
    System.exit(ret);
  }

  /**
   * Runs all the registered Alluxio Tests.
   *
   * @return the number of failed tests
   */
  private int runAlluxioTests() throws Exception {
    mDirectory = PathUtils.concatPath(mDirectory, TEST_DIRECTORY_NAME);

    AlluxioURI testDir = new AlluxioURI(mDirectory);
    FileSystemContext fsContext = FileSystemContext.create();
    FileSystem fs = FileSystem.Factory.create(fsContext);
    if (fs.exists(testDir)) {
      fs.delete(testDir, DeletePOptions.newBuilder().setRecursive(true).setUnchecked(true).build());
    }

    List<AlluxioTestCase> allTests = Lists.newArrayList();

    /*********************************************************************************************
     ************************** Please add your test cases here **********************************
     *********************************************************************************************/
    allTests.add(new CreateFileTest(mDirectory, fsContext));
    allTests.add(new CreateDirTest(mDirectory, fsContext));
    allTests.add(new ListStatusTest(mDirectory, fsContext));

    // Run test cases
    int failed = 0;
    for (AlluxioTestCase test : allTests) {
      fs.createDirectory(testDir);

      FileSystem filesystem = FileSystem.Factory.create(fsContext);
      Boolean pass;
      for (Map.Entry<String, Function<FileSystem, Boolean>> entry :
          test.getAllSubTests().entrySet()) {
        System.out.print(String.format("Running: %-30s[%-30s] ", test.toString(), entry.getKey()));
        Function<FileSystem, Boolean> subTest = entry.getValue();
        try {
          pass = subTest.apply(filesystem);
        } catch (Exception e) {
          pass = false;
        }
        RunTestUtils.printTestStatus(pass);
        if (!pass) {
          failed++;
        }
      }
      fs.delete(testDir, DeletePOptions.newBuilder().setRecursive(true).setUnchecked(true).build());
    }
    return failed;
  }
}
