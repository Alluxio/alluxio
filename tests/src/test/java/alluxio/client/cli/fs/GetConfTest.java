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
import static org.mockito.Matchers.any;

import alluxio.ClientContext;
import alluxio.SystemOutRule;
import alluxio.SystemPropertyRule;
import alluxio.cli.GetConf;
import alluxio.client.meta.RetryHandlingMetaMasterConfigClient;
import alluxio.conf.PropertyKey;
import alluxio.conf.ServerConfiguration;
import alluxio.grpc.ConfigProperty;
import alluxio.grpc.GetConfigurationPResponse;
import alluxio.wire.Configuration;

import com.google.common.collect.ImmutableMap;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link GetConf}.
 */
public final class GetConfTest {
  private ByteArrayOutputStream mOutputStream = new ByteArrayOutputStream();

  @Rule
  public SystemOutRule mOutputStreamRule = new SystemOutRule(mOutputStream);

  @After
  public void after() {
    ServerConfiguration.reset();
  }

  @Test
  public void getConf() throws Exception {
    ServerConfiguration.set(PropertyKey.WORKER_RAMDISK_SIZE, "2048");
    ClientContext ctx = ClientContext.create(ServerConfiguration.global());
    assertEquals(0, GetConf.getConf(ctx,
        PropertyKey.WORKER_RAMDISK_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    ServerConfiguration.set(PropertyKey.WORKER_RAMDISK_SIZE, "2MB");
    ctx = ClientContext.create(ServerConfiguration.global());
    assertEquals(0, GetConf.getConf(ctx,
        PropertyKey.WORKER_RAMDISK_SIZE.toString()));
    assertEquals("2MB\n", mOutputStream.toString());

    mOutputStream.reset();
    ServerConfiguration.set(PropertyKey.WORKER_RAMDISK_SIZE, "Nonsense");
    ctx = ClientContext.create(ServerConfiguration.global());
    assertEquals(0, GetConf.getConf(ctx,
        PropertyKey.WORKER_RAMDISK_SIZE.toString()));
    assertEquals("Nonsense\n", mOutputStream.toString());
  }

  @Test
  public void getConfByAlias() {
    PropertyKey testProperty = new PropertyKey.Builder("alluxio.test.property")
        .setAlias(new String[] {"alluxio.test.property.alias"})
        .setDefaultValue("testValue")
        .build();
    ClientContext ctx = ClientContext.create(ServerConfiguration.global());
    assertEquals(0, GetConf.getConf(ctx, "alluxio.test.property.alias"));
    assertEquals("testValue\n", mOutputStream.toString());

    mOutputStream.reset();
    assertEquals(0, GetConf.getConf(ctx, "alluxio.test.property"));
    assertEquals("testValue\n", mOutputStream.toString());
    PropertyKey.unregister(testProperty);
  }

  @Test
  public void getConfWithCorrectUnit() throws Exception {
    ServerConfiguration.set(PropertyKey.WORKER_RAMDISK_SIZE, "2048");
    ClientContext ctx = ClientContext.create(ServerConfiguration.global());
    assertEquals(0, GetConf.getConf(ctx, "--unit", "B",
        PropertyKey.WORKER_RAMDISK_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    ServerConfiguration.set(PropertyKey.WORKER_RAMDISK_SIZE, "2048");
    ctx = ClientContext.create(ServerConfiguration.global());
    assertEquals(0, GetConf.getConf(ctx, "--unit", "KB",
        PropertyKey.WORKER_RAMDISK_SIZE.toString()));
    assertEquals("2\n", mOutputStream.toString());

    mOutputStream.reset();
    ServerConfiguration.set(PropertyKey.WORKER_RAMDISK_SIZE, "2MB");
    ctx = ClientContext.create(ServerConfiguration.global());
    assertEquals(0, GetConf.getConf(ctx, "--unit", "KB",
        PropertyKey.WORKER_RAMDISK_SIZE.toString()));
    assertEquals("2048\n", mOutputStream.toString());

    mOutputStream.reset();
    ServerConfiguration.set(PropertyKey.WORKER_RAMDISK_SIZE, "2MB");
    ctx = ClientContext.create(ServerConfiguration.global());
    assertEquals(0, GetConf.getConf(ctx, "--unit", "MB",
        PropertyKey.WORKER_RAMDISK_SIZE.toString()));
    assertEquals("2\n", mOutputStream.toString());
  }

  @Test
  public void getConfWithWrongUnit() throws Exception {
    ServerConfiguration.set(PropertyKey.WORKER_RAMDISK_SIZE, "2048");
    assertEquals(1,
        GetConf.getConf(ClientContext.create(ServerConfiguration.global()), "--unit", "bad_unit",
            PropertyKey.WORKER_RAMDISK_SIZE.toString()));
  }

  @Test
  public void getConfWithInvalidConf() throws Exception {
    try (Closeable p = new SystemPropertyRule(ImmutableMap.of(
        PropertyKey.CONF_VALIDATION_ENABLED.toString(), "false",
        PropertyKey.ZOOKEEPER_ENABLED.toString(), "true")).toResource()) {
      ServerConfiguration.reset();
      ClientContext ctx = ClientContext.create(ServerConfiguration.global());
      assertEquals(0, GetConf.getConf(ctx,
          PropertyKey.ZOOKEEPER_ENABLED.toString()));
      assertEquals("true\n", mOutputStream.toString());
    } finally {
      ServerConfiguration.reset();
    }
  }

  @Test
  public void getConfFromMaster() throws Exception {
    // Prepare mock meta master client
    RetryHandlingMetaMasterConfigClient client =
        Mockito.mock(RetryHandlingMetaMasterConfigClient.class);
    Mockito.when(client.getConfiguration(any())).thenReturn(
        Configuration.fromProto(prepareGetConfigurationResponse()));

    assertEquals(0, GetConf.getConfImpl(() -> client, ServerConfiguration.global(), "--master"));
    String expectedOutput = "alluxio.logger.type=MASTER_LOGGER\n"
        + "alluxio.master.audit.logger.type=MASTER_AUDIT_LOGGER\n"
        + "alluxio.master.hostname=localhost\n"
        + "alluxio.master.mount.table.root.ufs=hdfs://localhost:9000\n"
        + "alluxio.master.rpc.port=19998\n"
        + "alluxio.master.web.port=19999\n";
    assertEquals(expectedOutput, mOutputStream.toString());
  }

  @Test
  public void getConfFromMasterWithSource() throws Exception {
    // Prepare mock meta master client
    RetryHandlingMetaMasterConfigClient client =
        Mockito.mock(RetryHandlingMetaMasterConfigClient.class);
    Mockito.when(client.getConfiguration(any())).thenReturn(Configuration.fromProto(
        prepareGetConfigurationResponse()));
    assertEquals(0, GetConf.getConfImpl(() -> client, ServerConfiguration.global(), "--master",
        "--source"));
    // CHECKSTYLE.OFF: LineLengthExceed - Much more readable
    String expectedOutput =
        "alluxio.logger.type=MASTER_LOGGER (SYSTEM_PROPERTY)\n"
        + "alluxio.master.audit.logger.type=MASTER_AUDIT_LOGGER (SYSTEM_PROPERTY)\n"
        + "alluxio.master.hostname=localhost (SITE_PROPERTY (/alluxio/conf/alluxio-site.properties))\n"
        + "alluxio.master.mount.table.root.ufs=hdfs://localhost:9000 (SITE_PROPERTY (/alluxio/conf/alluxio-site.properties))\n"
        + "alluxio.master.rpc.port=19998 (DEFAULT)\n"
        + "alluxio.master.web.port=19999 (DEFAULT)\n";
    // CHECKSTYLE.ON: LineLengthExceed
    assertEquals(expectedOutput, mOutputStream.toString());
  }

  /**
   * @return configuration info list to test
   */
  private List<ConfigProperty> prepareConfigList() {
    return Arrays.asList(
        ConfigProperty.newBuilder().setName("alluxio.master.rpc.port").setValue("19998")
            .setSource("DEFAULT").build(),
        ConfigProperty.newBuilder().setName("alluxio.master.web.port").setValue("19999")
            .setSource("DEFAULT").build(),
        ConfigProperty.newBuilder().setName("alluxio.master.hostname").setValue("localhost")
            .setSource("SITE_PROPERTY (/alluxio/conf/alluxio-site.properties)").build(),
        ConfigProperty.newBuilder().setName("alluxio.master.mount.table.root.ufs")
            .setValue("hdfs://localhost:9000")
            .setSource("SITE_PROPERTY (/alluxio/conf/alluxio-site.properties)").build(),
        ConfigProperty.newBuilder().setName("alluxio.logger.type").setValue("MASTER_LOGGER")
            .setSource("SYSTEM_PROPERTY").build(),
        ConfigProperty.newBuilder().setName("alluxio.master.audit.logger.type")
            .setValue("MASTER_AUDIT_LOGGER").setSource("SYSTEM_PROPERTY").build());
  }

  private GetConfigurationPResponse prepareGetConfigurationResponse() {
    return GetConfigurationPResponse.newBuilder()
        .addAllClusterConfigs(prepareConfigList())
        .build();
  }
}
