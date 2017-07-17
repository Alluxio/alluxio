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

package alluxio.cli;

import static org.junit.Assert.assertEquals;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.collections.Pair;
import alluxio.util.io.PathUtils;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.junit.rules.TemporaryFolder;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Tests for {@link alluxio.cli.ConfigurationDocGenerator}.
 */
@RunWith(Parameterized.class)
public class ConfigurationDocGeneratorTest {
  /**
   * Rule to create a new temporary folder during each test.
   */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();
  @Parameterized.Parameter
  public Pair<PropertyKey, String> mTestConf;
  private String mLocation;

  @Parameters
  public static Object[] data() {
    return new Object[]{
        new Pair<>(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES, "user-configuration.csv"),
        new Pair<>(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS, "master-configuration.csv"),
        new Pair<>(PropertyKey.WORKER_DATA_FOLDER, "worker-configuration.csv"),
        new Pair<>(PropertyKey.SECURITY_AUTHENTICATION_TYPE, "security-configuration.csv"),
        new Pair<>(PropertyKey.KEY_VALUE_PARTITION_SIZE_BYTES_MAX, "key-value-configuration.csv"),
        new Pair<>(PropertyKey.INTEGRATION_WORKER_RESOURCE_MEM, "common-configuration.csv")
    };
  }

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    mLocation = mFolder.newFolder().toString();
  }

  private void checkFileContents(String source, List<String> target) throws Exception {
    //assert file contents
    assertEquals(2, target.size());
    assertEquals(ConfigurationDocGenerator.FILE_HEADER, target.get(0));
    assertEquals(source, target.get(1));
  }

  @Test
  public void checkCSVFile() throws Exception {
    Collection<PropertyKey> defaultKeys = new ArrayList<>();
    PropertyKey pKey = mTestConf.getFirst();
    defaultKeys.add(pKey);

    ConfigurationDocGenerator.writeCSVFile(defaultKeys, mLocation);
    String filePath = PathUtils.concatPath(mLocation, mTestConf.getSecond());
    Path p = Paths.get(filePath);
    Assert.assertTrue(Files.exists(p));

    //assert file contents
    List<String> userFile = Files.readAllLines(p, StandardCharsets.UTF_8);
    String defaultValue = Configuration.get(pKey);
    checkFileContents(pKey + "," + defaultValue, userFile);
  }
}
