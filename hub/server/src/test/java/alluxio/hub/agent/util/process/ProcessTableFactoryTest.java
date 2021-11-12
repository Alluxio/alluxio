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

package alluxio.hub.agent.util.process;

import static org.junit.Assert.assertTrue;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class ProcessTableFactoryTest {

  @Test
  public void testLinuxTable() throws NoSuchFieldException, IllegalAccessException {
    Field f = getIsOsLinuxField();
    f.set(null, true);
    assertTrue(ProcessTable.Factory.getProcessTable() instanceof LinuxProcessTable);
  }

  @Test
  public void testMacOsTable() throws NoSuchFieldException, IllegalAccessException {
    Field f = getIsOsLinuxField();
    f.set(null, false);
    assertTrue(ProcessTable.Factory.getProcessTable() instanceof MacOsProcessTable);
  }

  private Field getIsOsLinuxField() throws NoSuchFieldException, IllegalAccessException {
    Field f = SystemUtils.class.getField("IS_OS_LINUX");
    f.setAccessible(true);
    // Remove final modifier
    Field modifiersField = Field.class.getDeclaredField("modifiers");
    modifiersField.setAccessible(true);
    modifiersField.setInt(f, f.getModifiers() & ~Modifier.FINAL);
    return f;
  }
}
