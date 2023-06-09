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

import static alluxio.client.file.cache.store.LocalPageStore.TEMP_DIR;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class LocalPageStoreTest {

  @Rule
  public TemporaryFolder mTemp = new TemporaryFolder();

  private PageStoreOptions mOptions;

  @Before
  public void before() {
    mOptions = new PageStoreOptions()
        .setStoreType(PageStoreType.LOCAL)
        .setRootDir(Paths.get(mTemp.getRoot().getAbsolutePath()));
  }

  @Test
  public void testPutGetDefault() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    helloWorldTest(pageStore);
  }

  @Test
  public void testSingleFileBucket() throws Exception {
    mOptions.setFileBuckets(1);
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    long numFiles = 100;
    for (int i = 0; i < numFiles; i++) {
      PageId id = new PageId(Integer.toString(i), 0);
      pageStore.put(id, "test".getBytes());
    }
    assertEquals(1, Files.list(
            Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize())))
        .count());
  }

  @Test
  public void testMultiFileBucket() throws Exception {
    int numBuckets = 10;
    mOptions.setFileBuckets(numBuckets);
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    long numFiles = numBuckets * 10;
    for (int i = 0; i < numFiles; i++) {
      PageId id = new PageId(Integer.toString(i), 0);
      pageStore.put(id, "test".getBytes());
    }
    assertEquals(10, Files.list(
            Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize())))
        .count());
  }

  @Test
  public void testAllTempPageWriteToTempFolder() throws Exception {
    int numBuckets = 10;
    mOptions.setFileBuckets(numBuckets);
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    long numFiles = numBuckets * 10;
    for (int i = 0; i < numFiles; i++) {
      PageId id = new PageId(Integer.toString(i), 0);
      pageStore.putTemporary(id, "test".getBytes());
      Path pageFile = pageStore.getPagePath(id, true);
      assertTrue(Files.exists(pageFile));
      Path parentFileDir = pageFile.getParent();
      Path bucketDir = parentFileDir.getParent();
      assertTrue(Files.exists(bucketDir));
      assertEquals(TEMP_DIR, bucketDir.getFileName().toString());
    }

    assertEquals(1, Files.list(
            Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize())))
        .count());
    assertEquals(numFiles, Files.list(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            TEMP_DIR)).count());
  }

  @Test
  public void testCommitTempFile() throws Exception {
    int numBuckets = 10;
    mOptions.setFileBuckets(numBuckets);
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    String tmpFileId = "tmp_file";
    PageId id0 = new PageId(tmpFileId, 0);
    pageStore.putTemporary(id0, "test0".getBytes());
    PageId id6 = new PageId(tmpFileId, 6);
    pageStore.putTemporary(id6, "test6".getBytes());
    assertTrue(Files.exists(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            TEMP_DIR, tmpFileId, "0")));
    assertTrue(Files.exists(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            TEMP_DIR, tmpFileId, "6")));
    pageStore.commit(tmpFileId);
    assertEquals(0, Files.list(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            TEMP_DIR)).count());
    assertTrue(Files.exists(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            PageStoreDir.getFileBucket(numBuckets, tmpFileId), tmpFileId, "0")));
    assertTrue(Files.exists(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            PageStoreDir.getFileBucket(numBuckets, tmpFileId), tmpFileId, "6")));
  }

  @Test
  public void testAbortTempFile() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    String tmpFileId = "tmp_file";
    PageId id0 = new PageId(tmpFileId, 0);
    pageStore.putTemporary(id0, "test0".getBytes());
    PageId id6 = new PageId(tmpFileId, 6);
    pageStore.putTemporary(id6, "test6".getBytes());
    String otherFile = "OtherFile";
    PageId otherFilePageId = new PageId(otherFile, 6);
    pageStore.putTemporary(otherFilePageId, "other".getBytes());
    assertTrue(Files.exists(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            TEMP_DIR, tmpFileId, "0")));
    assertTrue(Files.exists(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            TEMP_DIR, tmpFileId, "6")));
    pageStore.abort(tmpFileId);
    assertFalse(Files.exists(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            TEMP_DIR, tmpFileId)));
    assertTrue(Files.exists(
        Paths.get(mOptions.getRootDir().toString(), Long.toString(mOptions.getPageSize()),
            TEMP_DIR, otherFile)));
  }

  @Test
  public void cleanFileAndDirectory() throws Exception {
    LocalPageStore pageStore = new LocalPageStore(mOptions);
    PageId pageId = new PageId("0", 0);
    pageStore.put(pageId, "test".getBytes());
    Path p = pageStore.getPagePath(pageId, false);
    assertTrue(Files.exists(p));
    pageStore.delete(pageId);
    assertFalse(Files.exists(p));
    assertFalse(Files.exists(p.getParent()));
  }

  private void helloWorldTest(PageStore store) throws Exception {
    String msg = "Hello, World!";
    PageId id = new PageId("0", 0);
    store.put(id, msg.getBytes());
    byte[] buf = new byte[1024];
    assertEquals(msg.getBytes().length, store.get(id, new ByteArrayTargetBuffer(buf, 0)));
    assertArrayEquals(msg.getBytes(), Arrays.copyOfRange(buf, 0, msg.getBytes().length));
  }
}
