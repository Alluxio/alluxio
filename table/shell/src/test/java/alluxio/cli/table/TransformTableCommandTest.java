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

package alluxio.cli.table;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.cli.table.command.TransformTableCommand;
import alluxio.client.table.TableMasterClient;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Matchers;

/**
 * Test cases for TransformTableCommand.
 */
public class TransformTableCommandTest {

  @Test
  public void transform() throws Exception {
    transformInternal(null, "");
    transformInternal("-d abc", "abc");
  }

  private void transformInternal(String definition, String expected) throws Exception {
    TableMasterClient client = mock(TableMasterClient.class);
    when(client.transformTable(Matchers.anyString(), Matchers.anyString(), Matchers.anyString()))
        .thenReturn(0L);
    TransformTableCommand command =
        new TransformTableCommand(new InstancedConfiguration(ConfigurationUtils.defaults()),
            client, null);

    ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

    if (definition != null) {
      command.run(command.parseAndValidateArgs("db", "table", definition));
    } else {
      command.run(command.parseAndValidateArgs("db", "table"));
    }
    verify(client)
        .transformTable(Matchers.anyString(), Matchers.anyString(), argumentCaptor.capture());
    assertEquals(expected, argumentCaptor.getValue());
  }
}
