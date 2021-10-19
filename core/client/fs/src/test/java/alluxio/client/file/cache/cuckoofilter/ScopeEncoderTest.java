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

package alluxio.client.file.cache.cuckoofilter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import alluxio.client.quota.CacheScope;
import alluxio.test.util.ConcurrencyUtils;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class ScopeEncoderTest {
  private static final int BITS_PER_SCOPE = 8; // 256 scopes at most
  private static final int NUM_SCOPES = (1 << BITS_PER_SCOPE);
  private static final CacheScope SCOPE1 = CacheScope.create("schema1.table1");

  // concurrency configurations
  private static final int DEFAULT_THREAD_AMOUNT = 12;
  private static final int DEFAULT_TIMEOUT_SECONDS = 10;

  private ScopeEncoder mScopeEncoder;

  @Before
  public void init() {
    mScopeEncoder = new ScopeEncoder(BITS_PER_SCOPE);
  }

  @Test
  public void testBasic() {
    int id = mScopeEncoder.encode(SCOPE1);
    assertEquals(0, id);
    assertEquals(SCOPE1, mScopeEncoder.decode(id));
  }

  @Test
  public void testConcurrentEncodeDecode() throws Exception {
    List<Runnable> runnables = new ArrayList<>();
    for (int k = 0; k < DEFAULT_THREAD_AMOUNT; k++) {
      runnables.add(() -> {
        for (int i = 0; i < NUM_SCOPES * 16; i++) {
          int r = ThreadLocalRandom.current().nextInt(NUM_SCOPES);
          CacheScope scopeInfo = CacheScope.create("schema1.table" + r);
          int id = mScopeEncoder.encode(scopeInfo);
          assertEquals(scopeInfo, mScopeEncoder.decode(id));
          assertEquals(id, mScopeEncoder.encode(scopeInfo));
          assertTrue(0 <= id && id < NUM_SCOPES);
        }
      });
    }
    ConcurrencyUtils.assertConcurrent(runnables, DEFAULT_TIMEOUT_SECONDS);
  }
}
