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

import alluxio.AlluxioTestDirectory;
import alluxio.cli.Command;
import alluxio.cli.bundler.command.AbstractInfoCollectorCommand;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.ConfigurationUtils;

import org.junit.BeforeClass;
import org.junit.Test;
import org.reflections.Reflections;

import java.io.File;
import java.lang.reflect.Modifier;
import java.util.Collection;

public class InfoCollectorAllTest {
  private static InstancedConfiguration sConf =
          new InstancedConfiguration(ConfigurationUtils.defaults());

  @BeforeClass
  public static final void beforeClass() {
    File targetDir = AlluxioTestDirectory.createTemporaryDirectory("testDir");
    sConf.set(PropertyKey.CONF_DIR, targetDir);
  }

  private int getNumberOfCommands() {
    Reflections reflections =
            new Reflections(AbstractInfoCollectorCommand.class.getPackage().getName());
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
    InfoCollectorAll ica = new InfoCollectorAll(sConf);
    Collection<Command> commands = ica.getCommands();
    assertEquals(getNumberOfCommands(), commands.size());
  }
}
