package alluxio.cli.bundler.command;

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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CollectEnvCommandTest {
  private static InstancedConfiguration sConf;

  @BeforeClass
  public static void initConf() {
    sConf = InstancedConfiguration.defaults();
  }

  @Test
  public void linuxCmdExecuted() throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectEnvCommand cmd = new CollectEnvCommand(FileSystemContext.create(sConf));

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

    // Verify file context
    File[] files = subDir.listFiles();
    System.out.println(new String(Files.readAllBytes(files[0].toPath())));
  }
}
