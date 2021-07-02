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

package alluxio.client.cli.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.conf.ServerConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.cli.docgen.ConfigurationDocGenerator;
import alluxio.collections.Pair;
import alluxio.util.io.PathUtils;

import com.google.common.base.Joiner;
import org.apache.commons.lang3.StringUtils;
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
 * Tests for {@link ConfigurationDocGenerator}.
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
        {TYPE.CSV, new Pair<>(PropertyKey.HOME,
            "common-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES,
            "user-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.MASTER_WEB_PORT,
            "master-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.WORKER_DATA_FOLDER,
            "worker-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.SECURITY_AUTHENTICATION_TYPE,
            "security-configuration.csv")},
        {TYPE.CSV, new Pair<>(PropertyKey.INTEGRATION_WORKER_RESOURCE_MEM,
            "cluster-management-configuration.csv")},
        {TYPE.YML, new Pair<>(PropertyKey.HOME,
            "common-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.USER_LOCAL_READER_CHUNK_SIZE_BYTES,
            "user-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.MASTER_WEB_PORT,
            "master-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.WORKER_DATA_FOLDER,
            "worker-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.SECURITY_AUTHENTICATION_TYPE,
            "security-configuration.yml")},
        {TYPE.YML, new Pair<>(PropertyKey.INTEGRATION_WORKER_RESOURCE_MEM,
            "cluster-management-configuration.yml")}
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
    assertTrue(fType.equals(TYPE.CSV) || fType.equals(TYPE.YML));
    //assert file contents
    if (fType == TYPE.CSV) {
      assertEquals(2, target.size());
      assertEquals(ConfigurationDocGenerator.CSV_FILE_HEADER, target.get(0));
      assertEquals(source, target.get(1));
    } else if (fType == TYPE.YML) {
      assertEquals(StringUtils.countMatches(source, "\n") + 1, target.size());
      assertEquals(source, Joiner.on("\n").join(target));
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
    assertTrue(Files.exists(p));

    //assert file contents
    List<String> userFile = Files.readAllLines(p, StandardCharsets.UTF_8);
    String defaultValue = ServerConfiguration.get(pKey);
    checkFileContents(String.format("%s,\"%s\"", pKey, defaultValue), userFile, mFileType);
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
    assertTrue(Files.exists(p));

    //assert file contents
    List<String> keyDescription = Files.readAllLines(p, StandardCharsets.UTF_8);
    String expected = pKey + ":\n  '" + description.replace("'", "''") + "'";
    checkFileContents(expected, keyDescription, mFileType);
  }
}
