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

package alluxio.fuse.options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

public class MountCliOptionsTest {
  @Test
  public void parseKvPairs() throws Exception {
    MountCliOptions mountOptions = new MountCliOptions();
    JCommander jCommander = JCommander.newBuilder()
        .addObject(mountOptions)
        .build();
    jCommander.parse(
        "-o", "key=value",
        "-o", "list_option=a,b",
        "-o", "keyWithNoValue=");
    Map<String, String> optionsMap = mountOptions.getOptionsMap();
    assertEquals(Optional.of("value"), Optional.ofNullable(optionsMap.get("key")));
    assertEquals(Optional.of("a,b"), Optional.ofNullable(optionsMap.get("list_option")));
    assertEquals(Optional.of(""), Optional.ofNullable(optionsMap.get("keyWithNoValue")));
  }

  @Test
  public void parseKeyOnly() throws Exception {
    MountCliOptions mountOptions = new MountCliOptions();
    JCommander jCommander = JCommander.newBuilder()
        .addObject(mountOptions)
        .build();
    jCommander.parse("-o", "key1", "-o", "key2");
    Map<String, String> optionsMap = mountOptions.getOptionsMap();
    assertEquals(Optional.of(""), Optional.ofNullable(optionsMap.get("key1")));
    assertEquals(Optional.of(""), Optional.ofNullable(optionsMap.get("key2")));
  }

  @Test
  public void invalidEmptyValue() throws Exception {
    MountCliOptions mountOptions = new MountCliOptions();
    JCommander jCommander = JCommander.newBuilder()
        .addObject(mountOptions)
        .build();
    assertThrows(ParameterException.class, () -> jCommander.parse("-o", ""));
  }

  @Test
  public void invalidKvPair() throws Exception {
    MountCliOptions mountOptions = new MountCliOptions();
    JCommander jCommander = JCommander.newBuilder()
        .addObject(mountOptions)
        .build();
    assertThrows(ParameterException.class, () -> jCommander.parse("-o", "key=value1=value2"));
  }
}
