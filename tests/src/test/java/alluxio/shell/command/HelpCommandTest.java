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
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Tests for help command.
 */
public final class HelpCommandTest extends AbstractAlluxioShellTest {

  /**
   * Tests help which given command doesn't exist.
   */
  @Test
  public void helpNotExist() throws IOException {
    mFsShell.run("help", "notExistTestCommand");
    String expected = "notExistTestCommand is an unknown command.\n";
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests help command with given command which exists.
   */
  @Test
  public void help() throws IOException {
    mFsShell.run("help", "help");
    HelpCommand cmd = new HelpCommand(mFileSystem);
    String expected = String.format("%-60s%s%n", "       [" + cmd.getUsage() + "]   ",
            cmd.getDescription());
    Assert.assertEquals(expected, mOutput.toString());
  }

  /**
   * Tests help command without given command.
   */
  @Test
  public void helpAllCommand() throws IOException {
    mFsShell.run("help");
    final Map<String, ShellCommand> commands = new HashMap<>();
    String expected = "";
    AlluxioShellUtils.loadCommands(mFileSystem, commands);
    SortedSet<String> sortedCmds = new TreeSet<>(commands.keySet());
    for (String cmd : sortedCmds) {
      expected += String.format("%-60s%s%n", "       [" + commands.get(cmd).getUsage() + "]   ",
              commands.get(cmd).getDescription());
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
