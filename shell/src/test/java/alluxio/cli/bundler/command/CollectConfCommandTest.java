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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CollectConfCommandTest {
  public static final Set<String> FILE_NAMES = Stream.of(
          "alluxio-site.properties",
          "alluxio-site.properties.template",
          "alluxio-env.sh",
          "metrics.properties",
          "metrics.properties.template",
          "log4j.properties",
          "core-site.properties",
          "core-site.properties.template",
          "hdfs-site.properties",
          "master",
          "worker",
          "start-dfs.sh" // Some startup procedure not defined in Alluxio
  ).collect(Collectors.toSet());
  private Set<String> mExpectedFiles;
  private InstancedConfiguration mConf;
  private File mTestDir;

  @Before
  public void initConf() throws IOException {
    mExpectedFiles = new HashSet<>();
    mTestDir = prepareConfDir();
    mConf = InstancedConfiguration.defaults();
    mConf.set(PropertyKey.CONF_DIR, mTestDir.getAbsolutePath());
  }

  @After
  public void emptyLogDir() {
    mConf.unset(PropertyKey.CONF_DIR);
    mTestDir.delete();
  }

  // Prepare a temp dir with some log files
  private File prepareConfDir() throws IOException {
    // The dir path will contain randomness so will be different every time
    File testConfDir = InfoCollectorTestUtils.createTemporaryDirectory();

    for (String s : FILE_NAMES) {
      InfoCollectorTestUtils.createFileInDir(testConfDir, s);
      mExpectedFiles.add(s);
    }
    return testConfDir;
  }

  @Test
  public void confDirCopied() throws IOException, AlluxioException {
    CollectConfigCommand cmd = new CollectConfigCommand(FileSystemContext.create(mConf));

    File targetDir = InfoCollectorTestUtils.createTemporaryDirectory();
    CommandLine mockCommandLine = mock(CommandLine.class);
    String[] mockArgs = new String[]{cmd.getCommandName(), targetDir.getAbsolutePath()};
    when(mockCommandLine.getArgs()).thenReturn(mockArgs);
    int ret = cmd.run(mockCommandLine);
    Assert.assertEquals(0, ret);

    // Files will be copied to sub-dir of target dir
    File subDir = new File(Paths.get(targetDir.getAbsolutePath(),
            cmd.getCommandName()).toString());

    // alluxio-site.properties will be filtered since it contains unmasked credentials
    mExpectedFiles.remove("alluxio-site.properties");
    mExpectedFiles.remove("alluxio-site.properties.template");
    InfoCollectorTestUtils.verifyAllFiles(subDir, mExpectedFiles);
  }
}
