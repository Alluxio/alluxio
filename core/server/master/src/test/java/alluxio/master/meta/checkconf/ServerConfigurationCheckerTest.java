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

package alluxio.master.meta.checkconf;

import static org.junit.Assert.assertEquals;

import alluxio.PropertyKey;
import alluxio.wire.ConfigProperty;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

/**
 * Unit tests for {@link ServerConfigurationChecker}.
 */
public class ServerConfigurationCheckerTest {
  private ServerConfigurationRecord mRecordOne;
  private ServerConfigurationRecord mRecordTwo;
  private ServerConfigurationChecker mConfigChecker;

  @Before
  public void before() {
    mRecordOne = new ServerConfigurationRecord();
    mRecordTwo = new ServerConfigurationRecord();
    mConfigChecker = new ServerConfigurationChecker(mRecordOne, mRecordTwo);
  }

  @Test
  public void checkConf() {
    // Prepare data
    PropertyKey keyEnforce = new PropertyKey.Builder("TestKey1")
        .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.ENFORCE).build();
    ConfigProperty enforceProp = new ConfigProperty()
        .setName(keyEnforce.getName()).setSource("Test").setValue("Value");

    PropertyKey keyWarn = new PropertyKey.Builder("TestKey2")
        .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.WARN).build();
    ConfigProperty warnProp = new ConfigProperty()
        .setName(keyWarn.getName()).setSource("Test").setValue("Value2");

    String hostOne = RandomStringUtils.randomAlphanumeric(10);
    String hostTwo = RandomStringUtils.randomAlphanumeric(10);

    // When records have nothing different, no errors or warns will be found
    mRecordOne.registerNewConf(hostOne, Arrays.asList(enforceProp, warnProp));
    mRecordTwo.registerNewConf(hostTwo, Arrays.asList(enforceProp, warnProp));
    checkResults(0, 0, ServerConfigurationChecker.Status.PASSED);

    // When records have a wrong warn property, checker should be able to find config warns
    ConfigProperty wrongWarnProp = new ConfigProperty().setName(warnProp.getName())
        .setSource(warnProp.getSource()).setValue("WrongValue");
    mRecordOne.registerNewConf(hostOne, Arrays.asList(enforceProp, wrongWarnProp));
    checkResults(0, 1, ServerConfigurationChecker.Status.WARN);

    // When records have a wrong enforce property, checker should be able to find config errors
    ConfigProperty wrongEnforceProp = new ConfigProperty().setName(enforceProp.getName())
        .setSource(enforceProp.getSource()).setValue("WrongValue");
    mRecordTwo.registerNewConf(hostTwo, Arrays.asList(wrongEnforceProp, warnProp));
    checkResults(1, 1, ServerConfigurationChecker.Status.FAILED);
  }

  /**
   * Checks if the server-side configuration report is as expected.
   *
   * @param expectedErrorNum the expected error number
   * @param expectedWarnNum the expected warning number
   * @param expectedStatus the expected config check status
   */
  private void checkResults(int expectedErrorNum, int expectedWarnNum,
      ServerConfigurationChecker.Status expectedStatus) {
    mConfigChecker.checkConf();
    ServerConfigurationChecker.ConfigCheckReport report = mConfigChecker.getConfigCheckReport();
    assertEquals(expectedErrorNum, report.getConfigErrors().size());
    assertEquals(expectedWarnNum, report.getConfigWarns().size());
    assertEquals(expectedStatus, report.getConfigStatus());
  }
}
