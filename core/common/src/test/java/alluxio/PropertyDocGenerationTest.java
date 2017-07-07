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

package alluxio;

import static org.junit.Assert.assertEquals;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.charset.StandardCharsets;

/**
 * Tests for {@link PropertyDocGeneration}.
 */
public class PropertyDocGenerationTest {
  private static final Map<PropertyKey, Object> PROPERTY_KEY_TEST = new HashMap<>();
  private static final String FILE_HEADER = "propertyName,defaultValue";
  private String mLocation;

  /** Rule to create a new temporary folder during each test. */
  @Rule
  public TemporaryFolder mFolder = new TemporaryFolder();

  /**
   * Sets up all dependencies before a test runs.
   */
  @Before
  public void backupCSVFiles() throws Exception {
    mLocation = mFolder.newFolder().toString() + "/";
    System.out.println("-->tempfolder: " + mLocation);
  }

  /**
   * Cleans up the temp folder and files.
   */
  @After
  public void restoreCSVFiles() throws Exception {
    mFolder.delete();
  }

  public void checkFileContents(String source, List<String> target) throws Exception {
    //assert file contents
    assertEquals(2, target.size());
    assertEquals(FILE_HEADER, target.get(0));
    assertEquals(source, target.get(1));
    PROPERTY_KEY_TEST.clear();
  }

  @Test
  public void checkCSVFile_user() throws Exception {
    String key = "alluxio.user.local.reader.packet.size.bytes";
    PropertyKey userLocalWriterPacketSizeBytes = new PropertyKey(key);
    String defaultValue = Configuration.get(userLocalWriterPacketSizeBytes);
    PROPERTY_KEY_TEST.put(userLocalWriterPacketSizeBytes, defaultValue);
    PropertyDocGeneration.writeCSVFile((HashMap<PropertyKey, Object>) PROPERTY_KEY_TEST, mLocation);
    String filePath = mLocation + "user-configuration.csv";
    Path p = Paths.get(filePath);
    Assert.assertTrue(Files.exists(p));

    //assert file contents
    List<String> userFile = Files.readAllLines(p, StandardCharsets.UTF_8);
    checkFileContents(key + "," + defaultValue, userFile);
  }

  @Test
  public void checkCSVFile_master() throws Exception {
    String key = "alluxio.integration.master.resource.cpu";
    PropertyKey integrationMasterResourceCpu = new PropertyKey(key);
    String defaultValue = Configuration.get(integrationMasterResourceCpu);
    PROPERTY_KEY_TEST.put(integrationMasterResourceCpu, defaultValue);
    PropertyDocGeneration.writeCSVFile((HashMap<PropertyKey, Object>) PROPERTY_KEY_TEST, mLocation);
    String filePath = mLocation + "master-configuration.csv";
    Path p = Paths.get(filePath);
    Assert.assertTrue(Files.exists(p));

    //assert file contents
    List<String> userFile = Files.readAllLines(p, StandardCharsets.UTF_8);
    checkFileContents(key + "," + defaultValue, userFile);
  }

  @Test
  public void checkCSVFile_worker() throws Exception {
    String key = "alluxio.worker.data.folder";
    PropertyKey workerDataFolder = new PropertyKey(key);
    String defaultValue = Configuration.get(workerDataFolder);
    PROPERTY_KEY_TEST.put(workerDataFolder, defaultValue);
    PropertyDocGeneration.writeCSVFile((HashMap<PropertyKey, Object>) PROPERTY_KEY_TEST, mLocation);
    String filePath = mLocation + "worker-configuration.csv";
    Path p = Paths.get(filePath);
    Assert.assertTrue(Files.exists(p));

    //assert file contents
    List<String> userFile = Files.readAllLines(p, StandardCharsets.UTF_8);
    checkFileContents(key + "," + defaultValue, userFile);
  }

  @Test
  public void checkCSVFile_security() throws Exception {
    String key = "alluxio.security.authentication.type";
    PropertyKey securityAuthenticationType = new PropertyKey(key);
    String defaultValue = Configuration.get(securityAuthenticationType);
    PROPERTY_KEY_TEST.put(securityAuthenticationType, defaultValue);
    PropertyDocGeneration.writeCSVFile((HashMap<PropertyKey, Object>) PROPERTY_KEY_TEST, mLocation);
    String filePath = mLocation + "security-configuration.csv";
    Path p = Paths.get(filePath);
    Assert.assertTrue(Files.exists(p));

    //assert file contents
    List<String> userFile = Files.readAllLines(p, StandardCharsets.UTF_8);
    checkFileContents(key + "," + defaultValue, userFile);
  }

  @Test
  public void checkCSVFile_keyvalue() throws Exception {
    String key = "alluxio.keyvalue.enabled";
    PropertyKey keyValueEnabled = new PropertyKey(key);
    String defaultValue = Configuration.get(keyValueEnabled);
    PROPERTY_KEY_TEST.put(keyValueEnabled, defaultValue);
    PropertyDocGeneration.writeCSVFile((HashMap<PropertyKey, Object>) PROPERTY_KEY_TEST, mLocation);
    String filePath = mLocation + "key-value-configuration.csv";
    Path p = Paths.get(filePath);
    Assert.assertTrue(Files.exists(p));

    //assert file contents
    List<String> userFile = Files.readAllLines(p, StandardCharsets.UTF_8);
    checkFileContents(key + "," + defaultValue, userFile);
  }

  @Test
  public void checkCSVFile_common() throws Exception {
    String key = "alluxio.site.conf.dir";
    PropertyKey siteConfDir = new PropertyKey(key);
    String defaultValue = Configuration.get(siteConfDir);
    PROPERTY_KEY_TEST.put(siteConfDir, defaultValue);
    PropertyDocGeneration.writeCSVFile((HashMap<PropertyKey, Object>) PROPERTY_KEY_TEST, mLocation);
    String filePath = mLocation + "common-configuration.csv";
    Path p = Paths.get(filePath);
    System.out.println("-->filePath: " + filePath);
    System.out.println("-->Path p: " + p.toString());
    Assert.assertTrue(Files.exists(p));

    //assert file contents
    List<String> userFile = Files.readAllLines(p, StandardCharsets.UTF_8);
    checkFileContents(key + "," + defaultValue, userFile);
  }
}
