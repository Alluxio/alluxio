package alluxio.cli.bundler.command;

import alluxio.AlluxioTestDirectory;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;
import alluxio.shell.CommandReturn;
import org.apache.commons.cli.CommandLine;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


import static org.powermock.api.mockito.PowerMockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

public class CollectMetricsCommandTest {
  private static InstancedConfiguration sConf;
  private static FileSystemContext sContext;

  @BeforeClass
  public static void initConf() throws IOException, NoSuchFieldException {
    sConf = InstancedConfiguration.defaults();
    sContext = FileSystemContext.create(sConf);
    // TODO(jiacheng): Mock to localhost?
  }

  @Test
  public void metricsCollected() throws IOException, AlluxioException, NoSuchFieldException, IllegalAccessException {
    CollectMetricsCommand cmd = new CollectMetricsCommand(sContext);

    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);

    int ret = cmd.run(mockCommandLine);

    String[] filenames = targetDir.list();
    System.out.println(Arrays.toString(filenames));

    // Check output
    File contentDir = new File(targetDir, "collectMetrics");
    System.out.println(Arrays.toString(contentDir.list()));
    for (File f : contentDir.listFiles()) {
      String content = new String(Files.readAllBytes(f.toPath()));
      System.out.println(content);
    }
  }
}
