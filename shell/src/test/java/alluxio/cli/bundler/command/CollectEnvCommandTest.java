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

package alluxio.cli.bundler.command;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.AlluxioTestDirectory;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import alluxio.shell.ShellCommand;

import org.apache.commons.cli.CommandLine;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class CollectEnvCommandTest {
  private static InstancedConfiguration sConf;

  @BeforeClass
  public static void initConf() {
    sConf = InstancedConfiguration.defaults();
  }

  @Test
  public void linuxCmdExecuted()
          throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectEnvCommand cmd = new CollectEnvCommand(FileSystemContext.create(sConf));

    // Write to temp dir
    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    // Replace commands to execute
    Field f = cmd.getClass().getDeclaredField("mCommands");
    f.setAccessible(true);

    ShellCommand mockCommand = mock(ShellCommand.class);
    when(mockCommand.runWithOutput()).thenReturn(new CommandReturn(0, "nothing happens"));
    Map<String, ShellCommand> mockCommandMap = new HashMap<>();
    mockCommandMap.put("mockCommand", mockCommand);
    f.set(cmd, mockCommandMap);

    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Verify the command has been run
    verify(mockCommand).runWithOutput();

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    assertEquals(new String[]{"collectEnv.txt"}, subDir.list());

    // Verify the command output is found
    String fileContent = new String(Files.readAllBytes(subDir.listFiles()[0].toPath()));
    assertTrue(fileContent.contains("nothing happens"));
  }

  @Test
  public void betterCmdExecuted()
          throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectEnvCommand cmd = new CollectEnvCommand(FileSystemContext.create(sConf));

    // Write to temp dir
    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    // Replace commands to execute
    Field f = cmd.getClass().getDeclaredField("mCommands");
    f.setAccessible(true);
    ShellCommand mockCommand = mock(ShellCommand.class);
    when(mockCommand.runWithOutput()).thenReturn(
            new CommandReturn(0, "command normal executed"));
    Map<String, ShellCommand> mockCommandMap = new HashMap<>();
    mockCommandMap.put("mockCommand", mockCommand);
    f.set(cmd, mockCommandMap);

    // Replace better command to execute
    Field cb = cmd.getClass().getDeclaredField("mCommandsBetter");
    cb.setAccessible(true);
    ShellCommand mockCommandBetter = mock(ShellCommand.class);
    when(mockCommandBetter.runWithOutput()).thenReturn(
            new CommandReturn(0, "command better executed"));
    Map<String, ShellCommand> mockBetterMap = new HashMap<>();
    mockBetterMap.put("mockCommand", mockCommandBetter);
    cb.set(cmd, mockBetterMap);

    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Verify the better version command has been run
    verify(mockCommandBetter).runWithOutput();

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    assertEquals(new String[]{"collectEnv.txt"}, subDir.list());

    // Verify only the better version command output is found
    String fileContent = new String(Files.readAllBytes(subDir.listFiles()[0].toPath()));
    assertTrue(fileContent.contains("command better executed"));
  }
}
