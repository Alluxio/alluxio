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

import alluxio.cli.bundler.InfoCollectorTestUtils;
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

public class CollectAlluxioInfoCommandTest {
  private static InstancedConfiguration sConf;

  @BeforeClass
  public static void initConf() {
    sConf = InstancedConfiguration.defaults();
  }

  @Test
  public void alluxioCmdExecuted()
          throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectAlluxioInfoCommand cmd = new CollectAlluxioInfoCommand(FileSystemContext.create(sConf));

    // Write to temp dir
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{cmd.getCommandName(), targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    // Replace commands to execute
    Field f = cmd.getClass().getSuperclass().getDeclaredField("mCommands");
    f.setAccessible(true);

    CollectAlluxioInfoCommand.AlluxioCommand mockCommand =
            mock(CollectAlluxioInfoCommand.AlluxioCommand.class);
    when(mockCommand.runWithOutput())
            .thenReturn(new CommandReturn(0, "nothing happens"));
    Map<String, ShellCommand> mockCommandMap = new HashMap<>();
    mockCommandMap.put("mockCommand", mockCommand);
    f.set(cmd, mockCommandMap);

    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Verify the command has been run
    verify(mockCommand).runWithOutput();

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(),
            cmd.getCommandName()).toString());
    assertEquals(new String[]{"collectAlluxioInfo.txt"}, subDir.list());

    // Verify the command output is found
    String fileContent = new String(Files.readAllBytes(subDir.listFiles()[0].toPath()));
    assertTrue(fileContent.contains("nothing happens"));
  }

  @Test
  public void backupCmdExecuted()
          throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectAlluxioInfoCommand cmd = new CollectAlluxioInfoCommand(FileSystemContext.create(sConf));

    // Write to temp dir
    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{cmd.getCommandName(), targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    // Replace commands to execute
    Field f = cmd.getClass().getSuperclass().getDeclaredField("mCommands");
    f.setAccessible(true);
    CollectAlluxioInfoCommand.AlluxioCommand mockCommandFail =
            mock(CollectAlluxioInfoCommand.AlluxioCommand.class);
    when(mockCommandFail.runWithOutput()).thenReturn(
            new CommandReturn(255, "command failed"));
    Map<String, ShellCommand> mockCommandMap = new HashMap<>();
    mockCommandMap.put("mockCommand", mockCommandFail);
    f.set(cmd, mockCommandMap);

    // Replace better command to execute
    Field cb = cmd.getClass().getSuperclass().getDeclaredField("mCommandsAlt");
    cb.setAccessible(true);
    ShellCommand mockCommandBackup = mock(ShellCommand.class);
    when(mockCommandBackup.runWithOutput()).thenReturn(
            new CommandReturn(0, "backup command executed"));
    Map<String, ShellCommand> mockBetterMap = new HashMap<>();
    mockBetterMap.put("mockCommand", mockCommandBackup);
    cb.set(cmd, mockBetterMap);

    // The backup command worked so exit code is 0
    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Verify the 1st option command failed, then backup executed
    verify(mockCommandFail).runWithOutput();
    verify(mockCommandBackup).runWithOutput();

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    assertEquals(new String[]{"collectAlluxioInfo.txt"}, subDir.list());

    // Verify only the better version command output is found
    String fileContent = new String(Files.readAllBytes(subDir.listFiles()[0].toPath()));
    assertTrue(fileContent.contains("backup command executed"));
  }
}
