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

package alluxio.cli.fsadmin.command;

import alluxio.cli.fsadmin.report.ConfigurationCommand;
import alluxio.client.MetaMasterClient;
import alluxio.wire.ConfigProperty;

import org.hamcrest.collection.IsIterableContainingInOrder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConfigurationCommandTest {
  private MetaMasterClient mMetaMasterClient;

  @Before
  public void prepareDependencies() throws IOException {
    // Prepare mock meta master client
    mMetaMasterClient = Mockito.mock(MetaMasterClient.class);
    List<ConfigProperty> configList = prepareConfigList();
    Mockito.when(mMetaMasterClient.getConfiguration())
        .thenReturn(configList);
  }

  @Test
  public void configuration() throws IOException {
    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
         PrintStream printStream = new PrintStream(outputStream, true, "utf-8")) {
      ConfigurationCommand configurationCommand = new ConfigurationCommand(mMetaMasterClient,
          printStream);
      configurationCommand.run();
      String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
      List<String> expectedOutput = Arrays.asList("Alluxio configuration information: ",
          "Property                                 Value                                     "
              + "         Source                        ",
          "alluxio.master.port                      19998                                     "
              + "         DEFAULT                       ",
          "alluxio.master.web.port                  19999                                     "
              + "         DEFAULT                       ",
          "alluxio.master.hostname                  localhost                                 "
              + "         SITE_PROPERTY (/alluxio/conf/alluxio-site.properties)",
          "alluxio.underfs.address                  hdfs://localhost:9000                     "
              + "         SITE_PROPERTY (/alluxio/conf/alluxio-site.properties)",
          "alluxio.logger.type                      MASTER_LOGGER                             "
              + "         SYSTEM_PROPERTY               ",
          "alluxio.master.audit.logger.type         MASTER_AUDIT_LOGGER                       "
              + "         SYSTEM_PROPERTY               ");
      List<String> testOutput = Arrays.asList(output.split("\n"));
      Assert.assertThat(testOutput,
          IsIterableContainingInOrder.contains(expectedOutput.toArray()));
    }
  }

  /**
   * @return configuration info list to test
   */
  private List<ConfigProperty> prepareConfigList() {
    List<ConfigProperty> configList = new ArrayList<>();

    ConfigProperty firstConfigProperty = new ConfigProperty().setName("alluxio.master.port")
        .setValue("19998").setSource("DEFAULT");
    ConfigProperty secondConfigProperty = new ConfigProperty().setName("alluxio.master.web.port")
        .setValue("19999").setSource("DEFAULT");
    ConfigProperty thirdConfigProperty = new ConfigProperty()
        .setName("alluxio.master.hostname").setValue("localhost")
        .setSource("SITE_PROPERTY (/alluxio/conf/alluxio-site.properties)");
    ConfigProperty fourthConfigProperty = new ConfigProperty().setName("alluxio.underfs.address")
        .setValue("hdfs://localhost:9000")
        .setSource("SITE_PROPERTY (/alluxio/conf/alluxio-site.properties)");
    ConfigProperty fifthConfigProperty = new ConfigProperty().setName("alluxio.logger.type")
        .setValue("MASTER_LOGGER").setSource("SYSTEM_PROPERTY");
    ConfigProperty sixthConfigProperty = new ConfigProperty()
        .setName("alluxio.master.audit.logger.type")
        .setValue("MASTER_AUDIT_LOGGER").setSource("SYSTEM_PROPERTY");

    configList.add(firstConfigProperty);
    configList.add(secondConfigProperty);
    configList.add(thirdConfigProperty);
    configList.add(fourthConfigProperty);
    configList.add(fifthConfigProperty);
    configList.add(sixthConfigProperty);
    return configList;
  }
}
