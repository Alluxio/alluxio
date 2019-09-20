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

package alluxio.cli.fsadmin;

import alluxio.cli.Command;
import alluxio.conf.InstancedConfiguration;
import alluxio.util.ConfigurationUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class FsadminUnitTest {
  private static Map<String, Command> sCommands;

  @Test
  public void checkDocs() {
    InstancedConfiguration cmdCommand = new InstancedConfiguration(ConfigurationUtils.defaults());
    sCommands = new FileSystemAdminShell(cmdCommand).loadCommands();
    for (Map.Entry<String, Command> cmd : sCommands.entrySet()) {
      Command c = cmd.getValue();
      Assert.assertNotNull(c.getCommandName());
      Assert.assertNotNull(c.getUsage());
      Assert.assertNotNull(c.getDescription());
      Assert.assertNotNull(c.getExample());
    }
  }
}
