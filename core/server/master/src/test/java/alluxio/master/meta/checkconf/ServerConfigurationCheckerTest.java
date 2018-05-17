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
import alluxio.PropertyKey.Scope;
import alluxio.wire.Address;
import alluxio.wire.ConfigCheckReport;
import alluxio.wire.ConfigCheckReport.Status;
import alluxio.wire.ConfigProperty;

import org.apache.commons.lang.RandomStringUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

/**
 * Unit tests for {@link ServerConfigurationChecker}.
 */
public class ServerConfigurationCheckerTest {
  private ServerConfigurationStore mRecordOne;
  private ServerConfigurationStore mRecordTwo;
  private ServerConfigurationChecker mConfigChecker;

  @Before
  public void before() {
    mRecordOne = new ServerConfigurationStore();
    mRecordTwo = new ServerConfigurationStore();
    mConfigChecker = new ServerConfigurationChecker(mRecordOne, mRecordTwo);
  }

  @Test
  public void checkConf() {
    // Prepare data
    PropertyKey keyMasterEnforce = new PropertyKey.Builder("TestKey1")
        .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.ENFORCE)
        .setScope(Scope.MASTER).build();
    ConfigProperty masterEnforceProp = new ConfigProperty()
        .setName(keyMasterEnforce.getName()).setSource("Test").setValue("Value");

    PropertyKey keyWorkerWarn = new PropertyKey.Builder("TestKey2")
        .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.WARN)
        .setScope(Scope.WORKER).build();
    ConfigProperty workerWarnProp = new ConfigProperty()
        .setName(keyWorkerWarn.getName()).setSource("Test").setValue("Value");

    PropertyKey keyServerEnforce = new PropertyKey.Builder("TestKey3")
        .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.ENFORCE)
        .setScope(Scope.SERVER).build();
    ConfigProperty serverEnforceProp = new ConfigProperty()
        .setName(keyServerEnforce.getName()).setSource("Test").setValue("Value");

    Random random = new Random();
    Address addressOne = new Address(RandomStringUtils.randomAlphanumeric(10), random.nextInt());
    Address addressTwo = new Address(RandomStringUtils.randomAlphanumeric(10), random.nextInt());

    // When records have nothing different, no errors or warns will be found
    mRecordOne.registerNewConf(addressOne, Arrays.asList(masterEnforceProp, workerWarnProp));
    mRecordTwo.registerNewConf(addressTwo, Arrays.asList(masterEnforceProp, workerWarnProp));
    checkResults(0, 0, Status.PASSED);

    // When records have a wrong warn property, checker should be able to find config warns
    ConfigProperty wrongWorkerWarnProp = new ConfigProperty().setName(workerWarnProp.getName())
        .setSource(workerWarnProp.getSource()).setValue("WrongValue");
    mRecordOne.registerNewConf(addressOne, Arrays.asList(masterEnforceProp, wrongWorkerWarnProp));
    checkResults(0, 1, Status.WARN);

    // When records have a wrong enforce property, checker should be able to find config errors
    ConfigProperty wrongMasterEnforceProp = new ConfigProperty()
        .setName(masterEnforceProp.getName())
        .setSource(masterEnforceProp.getSource()).setValue("WrongValue");
    mRecordTwo.registerNewConf(addressTwo, Arrays.asList(wrongMasterEnforceProp, workerWarnProp));
    checkResults(1, 1, Status.FAILED);

    ConfigProperty wrongServerEnforceProp = new ConfigProperty()
        .setName(serverEnforceProp.getName())
        .setSource(serverEnforceProp.getSource()).setValue("WrongValue");
    mRecordOne.registerNewConf(addressOne,
        Arrays.asList(masterEnforceProp, workerWarnProp, serverEnforceProp));
    mRecordTwo.registerNewConf(addressTwo,
        Arrays.asList(masterEnforceProp, workerWarnProp, wrongServerEnforceProp));
    checkResults(1, 0, ConfigCheckReport.Status.FAILED);
  }

  /**
   * Checks if the server-side configuration report is as expected.
   *
   * @param expectedErrorNum the expected error number
   * @param expectedWarnNum the expected warning number
   * @param expectedStatus the expected config check status
   */
  private void checkResults(int expectedErrorNum, int expectedWarnNum,
      ConfigCheckReport.Status expectedStatus) {
    mConfigChecker.regenerateReport();
    ConfigCheckReport report = mConfigChecker.getConfigCheckReport();
    assertEquals(expectedErrorNum, report.getConfigErrors().size());
    assertEquals(expectedWarnNum, report.getConfigWarns().size());
    assertEquals(expectedStatus, report.getStatus());
  }
}
