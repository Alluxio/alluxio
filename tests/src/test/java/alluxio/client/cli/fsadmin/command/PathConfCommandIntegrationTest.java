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

package alluxio.client.cli.fsadmin.command;

import alluxio.cli.fsadmin.command.PathConfCommand;
import alluxio.cli.fsadmin.pathconf.AddCommand;
import alluxio.cli.fsadmin.pathconf.ListCommand;
import alluxio.cli.fsadmin.pathconf.RemoveCommand;
import alluxio.cli.fsadmin.pathconf.ShowCommand;
import alluxio.client.cli.fsadmin.AbstractFsAdminShellTest;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for pathConf command.
 */
public final class PathConfCommandIntegrationTest extends AbstractFsAdminShellTest {
  @Test
  public void noArg() {
    int ret = mFsAdminShell.run("pathConf");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(PathConfCommand.description(), lastLine(output));
  }

  @Test
  public void unknownSubCommand() {
    int ret = mFsAdminShell.run("pathConf", "unknown");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(PathConfCommand.description(), lastLine(output));
  }

  @Test
  public void addCommandWrongNumberOfArgs() {
    int ret = mFsAdminShell.run("pathConf", "add", "1", "2");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(AddCommand.description(), lastLine(output));
  }

  @Test
  public void addCommandUnknownOption() {
    int ret = mFsAdminShell.run("pathConf", "add", "--unknown");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(AddCommand.description(), lastLine(output));
  }

  @Test
  public void listCommandWrongNumberOfArgs() {
    int ret = mFsAdminShell.run("pathConf", "list", "1");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(ListCommand.description(), lastLine(output));
  }

  @Test
  public void listCommandUnknownOption() {
    int ret = mFsAdminShell.run("pathConf", "list", "--unknown");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(ListCommand.description(), lastLine(output));
  }

  @Test
  public void removeCommandWrongNumberOfArgs() {
    int ret = mFsAdminShell.run("pathConf", "remove", "1", "2");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(RemoveCommand.description(), lastLine(output));
  }

  @Test
  public void removeCommandUnknownOption() {
    int ret = mFsAdminShell.run("pathConf", "remove", "--unknown");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(RemoveCommand.description(), lastLine(output));
  }

  @Test
  public void showCommandWrongNumberOfArgs() {
    int ret = mFsAdminShell.run("pathConf", "show", "1", "2");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(ShowCommand.description(), lastLine(output));
  }

  @Test
  public void showCommandUnknownOption() {
    int ret = mFsAdminShell.run("pathConf", "show", "--unknown");
    Assert.assertEquals(-1, ret);
    String output = mOutput.toString();
    Assert.assertEquals(ShowCommand.description(), lastLine(output));
  }

  private String lastLine(String output) {
    String[] lines = output.split("\n");
    if (lines.length > 0) {
      return lines[lines.length - 1];
    }
    return "";
  }
}
