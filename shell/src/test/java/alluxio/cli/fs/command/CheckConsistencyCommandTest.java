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

import alluxio.exception.AlluxioException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

public class CheckConsistencyCommandTest {
  private ByteArrayOutputStream mOutput = new ByteArrayOutputStream();
  private ByteArrayOutputStream mError  = new ByteArrayOutputStream();

  @Before
  public void setupStreams() {
    System.setOut(new PrintStream(mOutput));
    System.setErr(new PrintStream(mError));
  }

  @After
  public void cleanupStreams() {
    System.setOut(null);
    System.setErr(null);
  }

  @Test
  public void testgetNumOfArgs() throws AlluxioException, IOException {
    CheckConsistencyCommand command = new CheckConsistencyCommand(null);
    int numOfArgs = command.getNumOfArgs();
    Assert.assertEquals(1, numOfArgs);
  }

  @Test
  public void testgetCommandName() throws AlluxioException, IOException {
    CheckConsistencyCommand command = new CheckConsistencyCommand(null);
    String commandName = command.getCommandName();
    String expectName = "checkConsistency";
    Assert.assertEquals(expectName, commandName);
  }

  @Test
  public void testgetUsage() throws AlluxioException, IOException {
    CheckConsistencyCommand command = new CheckConsistencyCommand(null);
    String commandUsage = command.getUsage();
    String expect = "checkConsistency [-r] <Alluxio path>";
    Assert.assertEquals(expect, commandUsage);
  }

  @Test
  public void testgetDescription() throws AlluxioException, IOException {
    CheckConsistencyCommand command = new CheckConsistencyCommand(null);
    String commandDesc = command.getDescription();
    String expect = "Checks the consistency of a persisted file or directory in Alluxio. Any"
            + " files or directories which only exist in Alluxio or do not match the metadata "
            + "of files in the under storage will be returned. An administrator should then "
            + "reconcile the differences.Specify -r to repair the inconsistent files.";
    Assert.assertEquals(expect, commandDesc);
  }
}
