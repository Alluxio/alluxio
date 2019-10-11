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

package alluxio.table.common.udb;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Map;

public class UdbConfigurationTest {

  @Test
  public void testGetMountOption() {
    Map<String, String> opts = new ImmutableMap.Builder<String, String>()
        .put("my.special.key", "myspecialvalue")
        .put("mountoption.(ufs://a).key1", "v1")
        .put("mountoption.(ufs://a).key2", "v2")
        .put("mountoption.(ufs://b).key2", "v3")
        .put("mountoption.(file).key2", "v4")
        .build();

    UdbConfiguration conf = new UdbConfiguration(opts);
    assertEquals(3, Whitebox.<Map<String, String>>getInternalState(conf, "mMountOptions").size());
    assertEquals(0, conf.getMountOption("").size());
    assertEquals(1, conf.getMountOption("ufs://b").size());
    assertEquals(2, conf.getMountOption("ufs://a").size());
    assertEquals(1, conf.getMountOption("file").size());
  }
}
