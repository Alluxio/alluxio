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

import alluxio.client.file.cache.FileInfo;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageInfo;
import alluxio.client.file.cache.PageStore;
import alluxio.client.quota.CacheScope;

import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class MemoryPageStoreTest {

  private MemoryPageStoreOptions mOptions;

  private static final byte[] TEST_PAGE = "test".getBytes();
  private static final int TEST_PAGE_SIZE = TEST_PAGE.length;
  private static final FileInfo TEST_FILE_INFO = new FileInfo(CacheScope.GLOBAL, 100);

  @Before
  public void before() {
    mOptions = new MemoryPageStoreOptions();
  }

  @Test
  public void testPutGetDefault() throws Exception {
    MemoryPageStore pageStore = new MemoryPageStore(mOptions);
    helloWorldTest(pageStore);
  }

  private void helloWorldTest(PageStore store) throws Exception {
    String msg = "Hello, World!";
    PageId id = new PageId("0", 0);
    PageInfo pageInfo = new PageInfo(id, TEST_PAGE_SIZE, TEST_FILE_INFO);
    store.put(pageInfo, msg.getBytes());
    byte[] buf = new byte[1024];
    assertEquals(msg.getBytes().length, store.get(pageInfo, buf));
    assertArrayEquals(msg.getBytes(), Arrays.copyOfRange(buf, 0, msg.getBytes().length));
  }
}
