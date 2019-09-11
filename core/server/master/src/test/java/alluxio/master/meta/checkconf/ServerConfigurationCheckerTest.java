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

import alluxio.conf.PropertyKey;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.ConfigStatus;
import alluxio.grpc.Scope;
import alluxio.wire.Address;
import alluxio.wire.ConfigCheckReport;

import org.apache.commons.lang3.RandomStringUtils;
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
    ConfigProperty masterEnforceProp = ConfigProperty.newBuilder()
        .setName(keyMasterEnforce.getName()).setSource("Test").setValue("Value").build();

    PropertyKey keyWorkerWarn = new PropertyKey.Builder("TestKey2")
        .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.WARN)
        .setScope(Scope.WORKER).build();
    ConfigProperty workerWarnProp = ConfigProperty.newBuilder()
        .setName(keyWorkerWarn.getName()).setSource("Test").setValue("Value").build();

    PropertyKey keyServerEnforce = new PropertyKey.Builder("TestKey3")
        .setConsistencyCheckLevel(PropertyKey.ConsistencyCheckLevel.ENFORCE)
        .setScope(Scope.SERVER).build();
    ConfigProperty serverEnforceProp = ConfigProperty.newBuilder()
        .setName(keyServerEnforce.getName()).setSource("Test").setValue("Value").build();

    Random random = new Random();
    Address addressOne = new Address(RandomStringUtils.randomAlphanumeric(10), random.nextInt());
    Address addressTwo = new Address(RandomStringUtils.randomAlphanumeric(10), random.nextInt());

    // When records have nothing different, no errors or warns will be found
    mRecordOne.registerNewConf(addressOne, Arrays.asList(masterEnforceProp, workerWarnProp));
    mRecordTwo.registerNewConf(addressTwo, Arrays.asList(masterEnforceProp, workerWarnProp));
    checkResults(0, 0, ConfigStatus.PASSED);

    // When records have a wrong warn property, checker should be able to find config warns
    ConfigProperty wrongWorkerWarnProp =
        ConfigProperty.newBuilder().setName(workerWarnProp.getName())
        .setSource(workerWarnProp.getSource()).setValue("WrongValue").build();
    mRecordOne.registerNewConf(addressOne, Arrays.asList(masterEnforceProp, wrongWorkerWarnProp));
    checkResults(0, 1, ConfigStatus.WARN);

    // When records have a wrong enforce property, checker should be able to find config errors
    ConfigProperty wrongMasterEnforceProp = ConfigProperty.newBuilder()
        .setName(masterEnforceProp.getName())
        .setSource(masterEnforceProp.getSource()).setValue("WrongValue").build();
    mRecordTwo.registerNewConf(addressTwo, Arrays.asList(wrongMasterEnforceProp, workerWarnProp));
    checkResults(1, 1, ConfigStatus.FAILED);

    ConfigProperty wrongServerEnforceProp = ConfigProperty.newBuilder()
        .setName(serverEnforceProp.getName())
        .setSource(serverEnforceProp.getSource()).setValue("WrongValue").build();
    mRecordOne.registerNewConf(addressOne,
        Arrays.asList(masterEnforceProp, workerWarnProp, serverEnforceProp));
    mRecordTwo.registerNewConf(addressTwo,
        Arrays.asList(masterEnforceProp, workerWarnProp, wrongServerEnforceProp));
    checkResults(1, 0, ConfigStatus.FAILED);
  }

  /**
   * Checks if the server-side configuration report is as expected.
   *
   * @param expectedErrorNum the expected error number
   * @param expectedWarnNum the expected warning number
   * @param expectedStatus the expected config check status
   */
  private void checkResults(int expectedErrorNum, int expectedWarnNum,
      ConfigStatus expectedStatus) {
    mConfigChecker.regenerateReport();
    ConfigCheckReport report = mConfigChecker.getConfigCheckReport();
    assertEquals(expectedErrorNum, report.getConfigErrors().size());
    assertEquals(expectedWarnNum, report.getConfigWarns().size());
    assertEquals(expectedStatus, report.getConfigStatus());
  }
}
