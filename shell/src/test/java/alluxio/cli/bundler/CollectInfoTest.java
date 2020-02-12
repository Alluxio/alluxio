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

package alluxio.cli.bundler;

import static org.junit.Assert.assertEquals;

import alluxio.cli.bundler.command.AbstractCollectInfoCommand;
import alluxio.conf.InstancedConfiguration;
import alluxio.cli.Command;
import alluxio.util.ConfigurationUtils;

import org.junit.Test;
import org.reflections.Reflections;

import java.lang.reflect.Modifier;
import java.util.Collection;

public class CollectInfoTest {
  private static InstancedConfiguration sConf =
          new InstancedConfiguration(ConfigurationUtils.defaults());

  private int getNumberOfCommands() {
    Reflections reflections =
            new Reflections(AbstractCollectInfoCommand.class.getPackage().getName());
    int cnt = 0;
    for (Class<? extends Command> cls : reflections.getSubTypesOf(Command.class)) {
      if (!Modifier.isAbstract(cls.getModifiers())) {
        cnt++;
      }
    }
    return cnt;
  }

  @Test
  public void loadedCommands() {
    CollectInfo ic = new CollectInfo(sConf);
    Collection<Command> commands = ic.getCommands();
    assertEquals(getNumberOfCommands(), commands.size());
  }
}
