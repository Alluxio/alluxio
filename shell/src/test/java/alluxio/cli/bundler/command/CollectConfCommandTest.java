package alluxio.cli.bundler.command;

import alluxio.AlluxioTestDirectory;
import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CollectConfCommandTest {
  private static InstancedConfiguration sConf;
  private static File sTestDir;

  @BeforeClass
  public static void initConf() throws IOException {
    sTestDir = prepareConfDir("testConf");
    sConf = InstancedConfiguration.defaults();
    sConf.set(PropertyKey.CONF_DIR, sTestDir.getAbsolutePath());
  }

  // Prepare a temp dir with some log files
  private static File prepareConfDir(String prefix) throws IOException {
    // The dir path will contain randomness so will be different every time
    File testConfDir = AlluxioTestDirectory.createTemporaryDirectory(prefix);
    InfoCollectorTestUtils.createFileInDir(testConfDir, "alluxio-site.properties");
    InfoCollectorTestUtils.createFileInDir(testConfDir, "alluxio-env.sh");
    return testConfDir;
  }

  @Test
  public void confDirCopied() throws IOException, AlluxioException {
    CollectConfigCommand cmd = new CollectConfigCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("confTarget");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    int ret = cmd.run(mockCommandLine);
    Assert.assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());

    // Check the dir copied
    String[] files = subDir.list();
    Arrays.sort(files);
    String[] expectedFiles = sTestDir.list();
    Arrays.sort(expectedFiles);
    Assert.assertEquals(expectedFiles, files);
  }

  @Test
  public void foundPreviousWork() throws IOException, AlluxioException {
    CollectConfigCommand cmd = new CollectConfigCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("confTarget");
    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    subDir.mkdir();
    // Put files in the target dir
    InfoCollectorTestUtils.createFileInDir(subDir, "test.txt");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    int ret = cmd.run(mockCommandLine);
    Assert.assertEquals(0, ret);

    // Check the log files are not copied
    String[] files = subDir.list();
    Assert.assertEquals(1, files.length);
    Assert.assertEquals("test.txt", files[0]);
  }

  @Test
  public void ignoredPreviousWork() throws IOException, AlluxioException {
    CollectConfigCommand cmd = new CollectConfigCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("confTarget");
    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(), cmd.getCommandName()).toString());
    subDir.mkdir();
    // Put files in the target dir
    InfoCollectorTestUtils.createFileInDir(subDir, "test.txt");

    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    when(mockCommandLine.hasOption("f")).thenReturn(true);

    int ret = cmd.run(mockCommandLine);
    Assert.assertEquals(0, ret);

    // The files are copied into the existing directory
    String[] files = subDir.list();
    Arrays.sort(files);
    String[] expectedFiles = (String[]) ArrayUtils.addAll(sTestDir.list(), new String[]{"test.txt"});
    Arrays.sort(expectedFiles);
    Assert.assertEquals(expectedFiles, files);
  }
}
