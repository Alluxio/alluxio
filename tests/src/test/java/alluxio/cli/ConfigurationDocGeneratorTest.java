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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Tests for {@link alluxio.cli.ConfigurationDocGenerator}.
 */
@RunWith(Parameterized.class)
public class ConfigurationDocGeneratorTest {
  enum TYPE {
    CSV,
    YML,
  }
  /**
   * Rule to create a new temporary folder during each test.
   */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();
  @Parameterized.Parameter
  public TYPE mFileType;
  @Parameterized.Parameter(1)
  public Pair<PropertyKey, String> mTestConf;
  private String mLocation;

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {TYPE.CSV, new Pair<>(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES,
            "user-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS,
            "master-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.WORKER_DATA_FOLDER,
            "worker-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.SECURITY_AUTHENTICATION_TYPE,
            "security-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.KEY_VALUE_PARTITION_SIZE_BYTES_MAX,
            "key-value-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.INTEGRATION_WORKER_RESOURCE_MEM,
            "common-configuration.csv")},
        {TYPE.YML, new Pair<>(PropertyKey.USER_LOCAL_READER_PACKET_SIZE_BYTES,
            "user-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.MASTER_CONNECTION_TIMEOUT_MS,
            "master-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.WORKER_DATA_FOLDER,
            "worker-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.SECURITY_AUTHENTICATION_TYPE,
            "security-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.KEY_VALUE_PARTITION_SIZE_BYTES_MAX,
            "key-value-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.INTEGRATION_WORKER_RESOURCE_MEM,
            "common-configuration.yml")}
    });
  }

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void before() throws Exception {
    mLocation = mFolder.newFolder().toString();
  }

  private void checkFileContents(String source, List<String> target, TYPE fType)
      throws Exception {
    Assert.assertTrue(fType.equals(TYPE.CSV) || fType.equals(TYPE.YML));
    //assert file contents
    if (fType == TYPE.CSV) {
      assertEquals(2, target.size());
      assertEquals(ConfigurationDocGenerator.CSV_FILE_HEADER, target.get(0));
      assertEquals(source, target.get(1));
    } else if (fType == TYPE.YML) {
      assertEquals(2, target.size());
      assertEquals(source, target.get(0) + "\n" + target.get(1));
    }
  }

  @Test
  public void checkCSVFile() throws Exception {
    if (mFileType != TYPE.CSV) {
      return;
    }
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
    checkFileContents(pKey + "," + defaultValue, userFile, mFileType);
  }

  @Test
  public void checkYMLFile() throws Exception {
    if (mFileType != TYPE.YML) {
      return;
    }
    Collection<PropertyKey> defaultKeys = new ArrayList<>();
    PropertyKey pKey = mTestConf.getFirst();
    String description = pKey.getDescription();
    defaultKeys.add(pKey);

    ConfigurationDocGenerator.writeYMLFile(defaultKeys, mLocation);
    String filePath = PathUtils.concatPath(mLocation, mTestConf.getSecond());
    Path p = Paths.get(filePath);
    Assert.assertTrue(Files.exists(p));

    //assert file contents
    List<String> keyDescription = Files.readAllLines(p, StandardCharsets.UTF_8);
    checkFileContents(pKey + ":\n  " + description, keyDescription, mFileType);
  }
}
