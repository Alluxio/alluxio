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

import java.util.Arrays;
import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({BaseFileSystem.class})
public class MetadataCacheTest {
  private static final AlluxioURI FILE = new AlluxioURI("/file");
  private static final URIStatus FILE_STATUS =
      new URIStatus(new FileInfo().setPath(FILE.getPath()));
  private static final AlluxioURI DIR1 = new AlluxioURI("/dir1");
  private static final URIStatus DIR1_STATUS =
      new URIStatus(new FileInfo().setPath(DIR1.getPath()));
  private static final AlluxioURI DIR1_FILE = new AlluxioURI("/dir1/file");
  private static final URIStatus DIR1_FILE_STATUS =
      new URIStatus(new FileInfo().setPath(DIR1_FILE.getPath()));
  private static final AlluxioURI DIR1_DIR2 = new AlluxioURI("/dir1/dir2");
  private static final URIStatus DIR1_DIR2_STATUS =
          new URIStatus(new FileInfo().setPath(DIR1_DIR2.getPath()));
  private static final AlluxioURI DIR1_DIR2_FILE = new AlluxioURI("/dir1/dir2/file");
  private static final URIStatus DIR1_DIR2_FILE_STATUS =
          new URIStatus(new FileInfo().setPath(DIR1_DIR2_FILE.getPath()));

  private MetadataCache mCache;

  @Test
  public void putAndGet() {
    mCache = new MetadataCache(100, Long.MAX_VALUE);
    assertEquals(0, mCache.size());

    mCache.put(FILE, FILE_STATUS);
    assertEquals(1, mCache.size());
    assertContain(FILE);

    mCache.put(DIR1, DIR1_STATUS);
    assertEquals(2, mCache.size());
    assertContain(FILE);
    assertContain(DIR1);

    mCache.put(DIR1_FILE, DIR1_FILE_STATUS);
    assertEquals(3, mCache.size());
    assertContain(FILE);
    assertContain(DIR1);
    assertContain(DIR1_FILE);
  }

  @Test
  public void fileStatusAndDirStatus() {
    mCache = new MetadataCache(100, Long.MAX_VALUE);
    assertEquals(0, mCache.size());
    // Puts dir and its children into cache.
    // All of them (DIR1, DIR1_FILE, DIR1_DIR2) should be in cache.
    mCache.put(DIR1, DIR1_STATUS);
    List<URIStatus> childrenInDIR1 = Arrays.asList(DIR1_FILE_STATUS, DIR1_DIR2_STATUS);
    mCache.put(DIR1, childrenInDIR1);
    assertEquals(3, mCache.size());
    assertNotNull(mCache.get(DIR1));
    assertNotNull(mCache.get(DIR1_DIR2));
    assertNotNull(mCache.get(DIR1_FILE));
    List<URIStatus> listDIR1 = mCache.listStatus(DIR1);
    assertEquals(childrenInDIR1, listDIR1);

    // Adds DIR1_DIR2_FILE into cache.
    List<URIStatus> childrenInDIR2 = Arrays.asList(DIR1_DIR2_FILE_STATUS);
    mCache.put(DIR1_DIR2, childrenInDIR2);
    assertEquals(4, mCache.size());
    assertNotNull(mCache.get(DIR1_DIR2));
    List<URIStatus> listDIR2 = mCache.listStatus(DIR1_DIR2);
    assertEquals(childrenInDIR2, listDIR2);
    // Checks parent directory
    assertNotNull(mCache.get(DIR1));
    assertNotNull(mCache.get(DIR1_DIR2));
    assertNotNull(mCache.get(DIR1_FILE));
    listDIR1 = mCache.listStatus(DIR1);
    assertEquals(childrenInDIR1, listDIR1);
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

    assertNotContain(DIR1_FILE);
    mCache.put(DIR1_FILE, DIR1_FILE_STATUS);
    assertContain(DIR1_FILE);
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
