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

package alluxio.client.file;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import alluxio.AlluxioURI;
import alluxio.wire.FileInfo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BaseFileSystem.class})
public class MetadataCacheTest {
  private static final AlluxioURI FILE = new AlluxioURI("/file");
  private static final URIStatus FILE_STATUS =
      new URIStatus(new FileInfo().setPath(FILE.getPath()));
  private static final AlluxioURI DIR = new AlluxioURI("/dir");
  private static final URIStatus DIR_STATUS =
      new URIStatus(new FileInfo().setPath(DIR.getPath()));
  private static final AlluxioURI DIR_FILE = new AlluxioURI("/dir/file");
  private static final URIStatus DIR_FILE_STATUS =
      new URIStatus(new FileInfo().setPath(DIR_FILE.getPath()));

  private MetadataCache mCache;

  @Test
  public void putAndGet() {
    mCache = new MetadataCache(100, Long.MAX_VALUE);
    assertEquals(0, mCache.size());

    mCache.put(FILE, FILE_STATUS);
    assertEquals(1, mCache.size());
    assertContain(FILE);

    mCache.put(DIR, DIR_STATUS);
    assertEquals(2, mCache.size());
    assertContain(FILE);
    assertContain(DIR);

    mCache.put(DIR_FILE, DIR_FILE_STATUS);
    assertEquals(3, mCache.size());
    assertContain(FILE);
    assertContain(DIR);
    assertContain(DIR_FILE);
  }

  @Test
  public void expire() throws Exception {
    // after writing to cache, expire after 1ms.
    mCache = new MetadataCache(100, 1);
    assertEquals(0, mCache.size());

    mCache.put(FILE, FILE_STATUS);

    Thread.sleep(2);
    // slept for 2ms, the cached item has expired.
    assertNotContain(FILE);
  }

  @Test
  public void evict() {
    // cache capacity is 1, evict the first cached item when the second is written.
    mCache = new MetadataCache(1, Long.MAX_VALUE);
    assertEquals(0, mCache.size());

    mCache.put(FILE, FILE_STATUS);
    assertContain(FILE);

    assertNotContain(DIR_FILE);
    mCache.put(DIR_FILE, DIR_FILE_STATUS);
    assertContain(DIR_FILE);
    // FILE is evicted due to capacity limit.
    assertNotContain(FILE);
  }

  private void assertContain(AlluxioURI path) {
    assertNotNull(mCache.get(path));
  }

  private void assertNotContain(AlluxioURI path) {
    assertNull(mCache.get(path));
  }
}
