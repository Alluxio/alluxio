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
import alluxio.annotation.dora.DoraTestTodoItem;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

@Ignore
@DoraTestTodoItem(action = DoraTestTodoItem.Action.FIX, owner = "jiacheng",
    comment = "pending security metadata in dora")
public class ChownCommandTest {
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
  public void chownPanicIllegalOwnerName() throws AlluxioException, IOException {
    ChownCommand command = new ChownCommand(null);

    String expectedOutput = String.format("Failed to parse user#1:group1 as user or user:group%n");
    verifyChownCommandReturnValueAndOutput(command, -1, expectedOutput,
        "user#1:group1", "/testFile");

    expectedOutput = String.format("Failed to parse user@1:group1 as user or user:group%n");
    verifyChownCommandReturnValueAndOutput(command, -1, expectedOutput,
        "user@1:group1", "/testFile");

    expectedOutput = String.format("Failed to parse 6user^$group$ as user or user:group%n");
    verifyChownCommandReturnValueAndOutput(command, -1, expectedOutput,
        "6user^$group$", "/testFile");
  }

  @Test
  public void chownPanicIllegalGroupName() throws AlluxioException, IOException {
    ChownCommand command = new ChownCommand(null);

    String expectedOutput = String.format("Failed to parse user1:^6group$ as user or user:group%n");
    verifyChownCommandReturnValueAndOutput(command, -1, expectedOutput,
        "user1:^6group$", "/testFile");
  }

  @Test
  public void chownLegalCases() throws AlluxioException, IOException {
    ChownCommand command = Mockito.spy(new ChownCommand(null));
    Mockito.doNothing().when(command).runWildCardCmd(
        Mockito.any(AlluxioURI.class), Mockito.any(CommandLine.class));
    verifyChownCommandReturnValueAndOutput(command, 0, "", "user-1:group-1", "/testFile");
    verifyChownCommandReturnValueAndOutput(command, 0, "", "user.1:group.1", "/testFile");
  }

  private void verifyChownCommandReturnValueAndOutput(ChownCommand command,
      int expectedReturnValue, String expectedOutput, String... args)
      throws AlluxioException, IOException {
    mOutput.reset();
    CommandLine cl = command.parseAndValidateArgs(args);
    int ret = command.run(cl);
    Assert.assertEquals(expectedReturnValue, ret);
    Assert.assertEquals(expectedOutput, mOutput.toString());
  }
}
