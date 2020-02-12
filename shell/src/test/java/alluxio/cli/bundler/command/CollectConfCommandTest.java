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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import alluxio.cli.bundler.InfoCollectorTestUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.AlluxioException;

import org.apache.commons.cli.CommandLine;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.Arrays;

public class CollectConfCommandTest {
  private static InstancedConfiguration sConf;
  private static File sTestDir;

  @BeforeClass
  public static void initConf() throws IOException {
    sTestDir = prepareConfDir();
    sConf = InstancedConfiguration.defaults();
    sConf.set(PropertyKey.CONF_DIR, sTestDir.getAbsolutePath());
  }

  // Prepare a temp dir with some log files
  private static File prepareConfDir() throws IOException {
    // The dir path will contain randomness so will be different every time
    File testConfDir = InfoCollectorTestUtils.createTemporaryDirectory();
    InfoCollectorTestUtils.createFileInDir(testConfDir, "alluxio-site.properties");
    InfoCollectorTestUtils.createFileInDir(testConfDir, "alluxio-env.sh");
    return testConfDir;
  }

  @Test
  public void confDirCopied() throws IOException, AlluxioException {
    CollectConfigCommand cmd = new CollectConfigCommand(FileSystemContext.create(sConf));

    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
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
}
