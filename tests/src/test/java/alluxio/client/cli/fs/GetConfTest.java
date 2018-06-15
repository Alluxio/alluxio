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

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.SystemOutRule;
import alluxio.cli.GetConf;
import alluxio.client.RetryHandlingMetaMasterClient;
import alluxio.wire.ConfigProperty;

import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link GetConf}.
 */
public final class GetConfTest {
  private ByteArrayOutputStream mOutputStream = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mOutputStreamRule = new SystemOutRule(mOutputStream);

  @Test
  public void getConf() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2048");
    assertEquals(0, GetConf.getConf(PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2MB");
    assertEquals(0, GetConf.getConf(PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2MB\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "Nonsense");
    assertEquals(0, GetConf.getConf(PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("Nonsense\n", mOutputStream.toString());
  }

  @Test
  public void getConfWithCorrectUnit() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2048");
    assertEquals(0, GetConf.getConf("--unit", "B", PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2048");
    assertEquals(0, GetConf.getConf("--unit", "KB", PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2MB");
    assertEquals(0, GetConf.getConf("--unit", "KB", PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2MB");
    assertEquals(0, GetConf.getConf("--unit", "MB", PropertyKey.WORKER_MEMORY_SIZE.toString()));
    assertEquals("2\n", mOutputStream.toString());
  }

  @Test
  public void getConfWithWrongUnit() throws Exception {
    Configuration.set(PropertyKey.WORKER_MEMORY_SIZE, "2048");
    assertEquals(1,
        GetConf.getConf("--unit", "bad_unit", PropertyKey.WORKER_MEMORY_SIZE.toString()));
  }

  @Test
  public void getConfFromMaster() throws Exception {
    // Prepare mock meta master client
    RetryHandlingMetaMasterClient client = Mockito.mock(RetryHandlingMetaMasterClient.class);
    List<ConfigProperty> configList = prepareConfigList();
    Mockito.when(client.getConfiguration()).thenReturn(configList);

    assertEquals(0, GetConf.getConfImpl(() -> client, "--master"));
    String expectedOutput = "alluxio.logger.type=MASTER_LOGGER\n"
        + "alluxio.master.audit.logger.type=MASTER_AUDIT_LOGGER\n"
        + "alluxio.master.hostname=localhost\n"
        + "alluxio.master.port=19998\n"
        + "alluxio.master.web.port=19999\n"
        + "alluxio.underfs.address=hdfs://localhost:9000\n";
    assertEquals(expectedOutput, mOutputStream.toString());
  }

  @Test
  public void getConfFromMasterWithSource() throws Exception {
    // Prepare mock meta master client
    RetryHandlingMetaMasterClient client = Mockito.mock(RetryHandlingMetaMasterClient.class);
    List<ConfigProperty> configList = prepareConfigList();
    Mockito.when(client.getConfiguration()).thenReturn(configList);
    assertEquals(0, GetConf.getConfImpl(() -> client, "--master", "--source"));
    // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
    String expectedOutput =
        "alluxio.logger.type=MASTER_LOGGER (SYSTEM_PROPERTY)\n"
        + "alluxio.master.audit.logger.type=MASTER_AUDIT_LOGGER (SYSTEM_PROPERTY)\n"
        + "alluxio.master.hostname=localhost (SITE_PROPERTY (/alluxio/conf/alluxio-site.properties))\n"
        + "alluxio.master.port=19998 (DEFAULT)\n"
        + "alluxio.master.web.port=19999 (DEFAULT)\n"
        + "alluxio.underfs.address=hdfs://localhost:9000 (SITE_PROPERTY (/alluxio/conf/alluxio-site.properties))\n";
    // CHECKSTYLE.ON: LineLengthExceed
    assertEquals(expectedOutput, mOutputStream.toString());
  }

  /**
   * @return configuration info list to test
   */
  private List<ConfigProperty> prepareConfigList() {
    return Arrays.asList(
        new ConfigProperty().setName("alluxio.master.port")
            .setValue("19998").setSource("DEFAULT"),
        new ConfigProperty().setName("alluxio.master.web.port")
            .setValue("19999").setSource("DEFAULT"),
        new ConfigProperty().setName("alluxio.master.hostname").setValue("localhost")
            .setSource("SITE_PROPERTY (/alluxio/conf/alluxio-site.properties)"),
        new ConfigProperty().setName("alluxio.underfs.address")
            .setValue("hdfs://localhost:9000")
            .setSource("SITE_PROPERTY (/alluxio/conf/alluxio-site.properties)"),
        new ConfigProperty().setName("alluxio.logger.type")
            .setValue("MASTER_LOGGER").setSource("SYSTEM_PROPERTY"),
        new ConfigProperty().setName("alluxio.master.audit.logger.type")
            .setValue("MASTER_AUDIT_LOGGER").setSource("SYSTEM_PROPERTY")
    );
  }
}
