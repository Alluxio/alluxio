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

package alluxio.client.file.cache.store;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;
import alluxio.file.ByteArrayTargetBuffer;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class MemoryPageStoreTest {
  private static final int PAGE_SIZE = 1024;
  private PageStoreOptions mOptions;

  @Before
  public void before() {
    mOptions = new PageStoreOptions();
  }

  @Test
  public void testPutGetDefault() throws Exception {
    MemoryPageStore pageStore = new MemoryPageStore(PAGE_SIZE);
    helloWorldTest(pageStore);
  }

  private void helloWorldTest(PageStore store) throws Exception {
    String msg = "Hello, World!";
    PageId id = new PageId("0", 0);
    store.put(id, msg.getBytes());
    byte[] buf = new byte[PAGE_SIZE];
    assertEquals(msg.getBytes().length, store.get(id, new ByteArrayTargetBuffer(buf, 0)));
    assertArrayEquals(msg.getBytes(), Arrays.copyOfRange(buf, 0, msg.getBytes().length));
  }
}
