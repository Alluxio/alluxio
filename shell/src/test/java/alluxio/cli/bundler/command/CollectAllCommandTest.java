package alluxio.cli.bundler.command;

import alluxio.AlluxioTestDirectory;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.exception.AlluxioException;
import org.apache.commons.cli.CommandLine;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CollectAllCommandTest {
  private static InstancedConfiguration sConf;

  @BeforeClass
  public static void initConf() {
    sConf = InstancedConfiguration.defaults();
  }

  @Test
  public void linuxCmdExecuted() throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectAllCommand cmd = new CollectAllCommand(FileSystemContext.create(sConf));

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    int ret = cmd.run(mockCommandLine);

    // The final result will be in a tarball

  }
}
