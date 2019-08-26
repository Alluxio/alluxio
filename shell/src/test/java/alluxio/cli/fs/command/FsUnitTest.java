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

package alluxio.cli.fs.command;

import alluxio.cli.Command;
import alluxio.cli.fs.FileSystemShellUtils;
import alluxio.client.file.FileSystemContext;
import alluxio.conf.ServerConfiguration;

import com.google.common.io.Closer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class FsUnitTest {
  private static Map<String, Command> sCommands;

  @Before
  public void loadFsCommands() {
    Closer mCloser = Closer.create();
    sCommands = FileSystemShellUtils.loadCommands(
            mCloser.register(FileSystemContext.create(ServerConfiguration.global())));
  }

  @Test
  public void checkDocs() {
    for (Map.Entry<String, Command> cmd : sCommands.entrySet()) {
      Command c = cmd.getValue();
      Assert.assertNotNull(c.getCommandName());
      Assert.assertNotNull(c.getUsage());
      Assert.assertNotNull(c.getDescription());
      Assert.assertNotNull(c.getExample());
    }
  }
}
