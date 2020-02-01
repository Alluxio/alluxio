package alluxio.cli.bundler.command;

import alluxio.AlluxioTestDirectory;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CollectJvmInfoCommandTest {
  private static InstancedConfiguration sConf;

  @BeforeClass
  public static void initConf() throws IOException {
    sConf = InstancedConfiguration.defaults();
  }

  @Test
  public void jpsCmdExecuted() throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectJvmInfoCommand cmd = new CollectJvmInfoCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    int ret = cmd.run(mockCommandLine);

    String[] filenames = targetDir.list();
    System.out.println(Arrays.toString(filenames));

    // Check output
    File contentDir = new File(targetDir, "collectJvmInfo");
    System.out.println(Arrays.toString(contentDir.list()));
  }
}
