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
import java.util.ArrayList;
import java.util.List;

public class CollectAlluxioInfoCommandTest {
  private static InstancedConfiguration sConf;

  @BeforeClass
  public static void initConf() {
    sConf = InstancedConfiguration.defaults();
  }

  @Test
  public void alluxioCmdExecuted()
          throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectAlluxioInfoCommand cmd =
            new CollectAlluxioInfoCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    // Replace commands to execute
    Field f = cmd.getClass().getDeclaredField("mAlluxioCommands");
    f.setAccessible(true);
    CollectAlluxioInfoCommand.AlluxioCommand mockCommand =
            mock(CollectAlluxioInfoCommand.AlluxioCommand.class);
    when(mockCommand.runWithOutput()).thenReturn(new CommandReturn(0, "nothing happens"));
    List<CollectAlluxioInfoCommand.AlluxioCommand> mockCommandList = new ArrayList<>();
    mockCommandList.add(mockCommand);
    f.set(cmd, mockCommandList);

    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Verify the command has been run
    verify(mockCommand).runWithOutput();

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    assertEquals(new String[]{"alluxioInfo.txt"}, subDir.list());

    // Verify file context
    String fileContent = new String(Files.readAllBytes(subDir.listFiles()[0].toPath()));
    assertTrue(fileContent.contains("nothing happens"));
  }

  @Test
  public void alternativeAlluxioCmdExecuted()
          throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectAlluxioInfoCommand cmd =
            new CollectAlluxioInfoCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    // Replace commands to execute
    Field f = cmd.getClass().getDeclaredField("mAlluxioCommands");
    f.setAccessible(true);

    ShellCommand mockSucceedCommand = mock(ShellCommand.class);
    when(mockSucceedCommand.runWithOutput()).thenReturn(new CommandReturn(0, "this worked"));

    CollectAlluxioInfoCommand.AlluxioCommand mockFailedCommand =
            mock(CollectAlluxioInfoCommand.AlluxioCommand.class);
    when(mockFailedCommand.runWithOutput()).thenReturn(new CommandReturn(1, "this failed"));
    when(mockFailedCommand.hasAlternativeCommand()).thenReturn(true);
    when(mockFailedCommand.getAlternativeCommand()).thenReturn(mockSucceedCommand);

    List<CollectAlluxioInfoCommand.AlluxioCommand> mockCommandList = new ArrayList<>();
    mockCommandList.add(mockFailedCommand);
    f.set(cmd, mockCommandList);

    int ret = cmd.run(mockCommandLine);
    assertEquals(0, ret);

    // Verify the command has been run
    verify(mockSucceedCommand).runWithOutput();

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    assertEquals(new String[]{"alluxioInfo.txt"}, subDir.list());

    // Verify file context
    String fileContent = new String(Files.readAllBytes(subDir.listFiles()[0].toPath()));
    assertTrue(fileContent.contains("this worked"));
  }
}
