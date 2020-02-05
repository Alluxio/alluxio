package alluxio.cli.bundler.command;

import alluxio.AlluxioTestDirectory;
import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
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
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CollectAlluxioInfoCommandTest {
  private static InstancedConfiguration sConf;
  private static File sTestDir;

  @BeforeClass
  public static void initConf() throws IOException {
    sTestDir = prepareLogDir("testLog");
    sConf = InstancedConfiguration.defaults();
    sConf.set(PropertyKey.LOGS_DIR, sTestDir.getAbsolutePath());
  }

  // Prepare a temp dir with some log files
  private static File prepareLogDir(String prefix) throws IOException {
    // The dir path will contain randomness so will be different every time
    File testConfDir = AlluxioTestDirectory.createTemporaryDirectory(prefix);
    InfoCollectorTestUtils.createFileInDir(testConfDir, "master.log");
    InfoCollectorTestUtils.createFileInDir(testConfDir, "worker.log");
    return testConfDir;
  }

  @Test
  public void alluxioCmdExecuted() throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectAlluxioInfoCommand cmd = new CollectAlluxioInfoCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    // Replace commands to execute
    Field f = cmd.getClass().getDeclaredField("mCommands");
    f.setAccessible(true);
    CollectAlluxioInfoCommand.AlluxioCommand mockCommand = mock(CollectAlluxioInfoCommand.AlluxioCommand.class);
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
    File[] files = subDir.listFiles();
    System.out.println(new String(Files.readAllBytes(files[0].toPath())));
  }

  @Test
  public void alternativeAlluxioCmdExecuted() throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectAlluxioInfoCommand cmd = new CollectAlluxioInfoCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    // Replace commands to execute
    Field f = cmd.getClass().getDeclaredField("mCommands");
    f.setAccessible(true);

    ShellCommand mockSucceedCommand = mock(ShellCommand.class);
    when(mockSucceedCommand.runWithOutput()).thenReturn(new CommandReturn(0, "this worked"));

    CollectAlluxioInfoCommand.AlluxioCommand mockFailedCommand = mock(CollectAlluxioInfoCommand.AlluxioCommand.class);
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
    File[] files = subDir.listFiles();
    System.out.println(new String(Files.readAllBytes(files[0].toPath())));
  }


  @Test
  public void foundPreviousWork() throws IOException, AlluxioException {
  }

  @Test
  public void ignoredPreviousWork() throws IOException, AlluxioException {
  }
}
