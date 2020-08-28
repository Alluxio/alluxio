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

package alluxio.shell;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import alluxio.AlluxioTestDirectory;
import alluxio.util.ShellUtils;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

public class ShellCommandTest {
  public static File createFileInDir(File dir, String fileName) throws IOException {
    File newFile = new File(Paths.get(dir.getAbsolutePath(), fileName).toString());
    newFile.createNewFile();
    return newFile;
  }

  @Rule
  public ExpectedException mExceptionRule = ExpectedException.none();

  /**
   * Tests the {@link ShellUtils#execCommand(String...)} method.
   *
   * @throws Throwable when the execution of the command fails
   */
  @Test
  public void execCommand() throws Exception {
    String testString = "alluxio";
    // Execute echo for testing command execution.
    String[] cmd = new String[]{"bash", "-c", "echo " + testString};
    String result = new ShellCommand(cmd).run();
    assertEquals(testString + "\n", result);
  }

  @Test
  public void execCommandFail() throws Exception {
    mExceptionRule.expect(ShellUtils.ExitCodeException.class);
    // run a command that guarantees to fail
    String[] cmd = new String[]{"bash", "-c", "false"};
    String result = new ShellCommand(cmd).run();
    assertEquals("false\n", result);
  }

  @Test
  public void execCommandTolerateFailureSucceed() throws Exception {
    // create temp file
    File testDir = AlluxioTestDirectory.createTemporaryDirectory("command");

    File testFile = createFileInDir(testDir, "testFile");

    // ls temp file
    String[] testCommandSucceed = new String[]{"ls",
            String.format("%s", testDir.getAbsolutePath())};
    CommandReturn crs = new ShellCommand(testCommandSucceed).runWithOutput();
    assertEquals(0, crs.getExitCode());
    assertTrue(crs.getOutput().contains(testFile.getName()));
  }

  @Test
  public void execCommandTolerateFailureFailed() throws Exception {
    // create temp file
    File testDir = AlluxioTestDirectory.createTemporaryDirectory("command");

    // do sth wrong
    String[] testCommandFail = new String[]{"ls",
            String.format("%saaaa", testDir.getAbsolutePath())};
    CommandReturn crf = new ShellCommand(testCommandFail).runWithOutput();
    assertNotEquals(0, crf.getExitCode());
    // The error is redirected into stdout
    assertTrue(crf.getOutput().length() > 0);
    assertNotEquals("", crf.getOutput());
  }

  @Test
  public void execCommandTolerateFailureInvalidCommand() throws Exception {
    // create temp file
    File testDir = AlluxioTestDirectory.createTemporaryDirectory("command");
    // For a non-existent command the command return contains the err msg
    String[] testCommandExcept = new String[]{"lsa",
            String.format("%s", testDir.getAbsolutePath())};
    // lsa is not a valid executable
    CommandReturn crf = new ShellCommand(testCommandExcept).runWithOutput();
    System.out.println(crf.getFormattedOutput());
    assertNotEquals(0, crf.getExitCode());
    assertTrue(crf.getOutput().contains("No such file or directory"));
  }
}
