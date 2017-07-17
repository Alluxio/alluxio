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

import alluxio.shell.AbstractAlluxioShellTest;
import alluxio.shell.AlluxioShellUtils;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Integration tests for help command.
 */
public final class HelpCommandIntegrationTest extends AbstractAlluxioShellTest {

  /**
   * Tests help which given command doesn't exist.
   */
  @Test
  public void helpNotExist() throws IOException {
    Assert.assertEquals(-1, mFsShell.run("help", "notExistTestCommand"));
    String expected = "notExistTestCommand is an unknown command.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests help command with given command which exists.
   */
  @Test
  public void help() throws IOException {
    Assert.assertEquals(0, mFsShell.run("help", "help"));
    HelpCommand cmd = new HelpCommand(mFileSystem);
    StringWriter stringWriter = new StringWriter();
    PrintWriter printWriter = new PrintWriter(stringWriter);
    HelpCommand.printCommandInfo(cmd, printWriter);
    printWriter.close();
    String expected = stringWriter.toString();
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests help command without given command.
   */
  @Test
  public void helpAllCommand() throws IOException {
    Assert.assertEquals(0, mFsShell.run("help"));
    final Map<String, ShellCommand> commands = AlluxioShellUtils.loadCommands(mFileSystem);
    String expected = "";
    SortedSet<String> sortedCmds = new TreeSet<>(commands.keySet());
    for (String cmd : sortedCmds) {
      ShellCommand command = commands.get(cmd);
      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);
      HelpCommand.printCommandInfo(command, printWriter);
      printWriter.close();
      expected += stringWriter.toString() + "\n";
    }
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests help command with redundant args.
   */
  @Test
  public void helpRedundantArgs() throws IOException {
    Assert.assertEquals(-1, mFsShell.run("help", "Cat", "Chmod"));
  }
}
