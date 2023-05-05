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

package alluxio.client.file.cache;

import alluxio.AlluxioURI;
import alluxio.CloseableSupplier;
import alluxio.Constants;
import alluxio.PositionReader;
import alluxio.PositionReaderTest;
import alluxio.client.file.CacheContext;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.MemoryPageStore;
import alluxio.client.file.cache.store.MemoryPageStoreDir;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.file.FileId;
import alluxio.underfs.UnderFileSystem;
import alluxio.underfs.UnderFileSystemConfiguration;
import alluxio.underfs.local.LocalPositionReader;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

/**
 * Tests for the {@link LocalCacheManager} class.
 * TODO(Beinan): Use parameterized LocalCacheManagerTest instead this test class
 */
@RunWith(Parameterized.class)
public final class LocalCachePositionReaderTest {

  @Parameterized.Parameters(name = "{index}-{0}")
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][] {
        {0},
        {1},
        {128},
        {256},
        {666},
        {5314},
        { 1 * Constants.KB - 1},
        { 1 * Constants.KB},
        { 1 * Constants.KB + 1},
        { 64 * Constants.KB - 1},
        { 64 * Constants.KB},
        { 64 * Constants.KB + 1},
    });
  }

  @Parameterized.Parameter
  public int mFileLen;

  private UnderFileSystem mLocalUfs;

  private LocalCacheManager mCacheManager;
  private InstancedConfiguration mConf = Configuration.copyGlobal();

  private String mTestFile;
  private PositionReader mPositionReader;
  private PositionReaderTest mPositionReaderTest;

  @Rule
  public TemporaryFolder mTemporaryFolder = new TemporaryFolder();

  @Before
  public void before() throws Exception {
    int pageSize = Constants.KB;
    mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, pageSize);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(10 * pageSize));
    mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, false);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, 0);
    mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.MEM);
    PageStoreOptions pageStoreOptions = PageStoreOptions.create(mConf).get(0);
    CacheEvictor evictor = new FIFOCacheEvictor(new CacheEvictorOptions());
    PageStoreDir pageStoreDir = new MemoryPageStoreDir(pageStoreOptions,
        (MemoryPageStore) PageStore.create(pageStoreOptions), evictor);
    PageMetaStore pageMetaStore = new DefaultPageMetaStore(ImmutableList.of(pageStoreDir));
    mCacheManager = createLocalCacheManager(mConf, pageMetaStore);
    String localUfsRoot = mTemporaryFolder.getRoot().getAbsolutePath();

    mLocalUfs =
        UnderFileSystem.Factory.create(localUfsRoot, UnderFileSystemConfiguration.defaults(mConf));
    Path path = Paths.get(localUfsRoot, "testFile" + UUID.randomUUID());
    try (FileOutputStream os = new FileOutputStream(path.toFile())) {
      os.write(BufferUtils.getIncreasingByteArray(mFileLen));
    }
    mTestFile = path.toString();
    mPositionReader = LocalCachePositionReader.create(mCacheManager,
        new CloseableSupplier<>(() -> new LocalPositionReader(mTestFile, mFileLen)),
        FileId.of(new AlluxioURI(mTestFile).hash()), mFileLen, pageSize,
        CacheContext.defaults());
    mPositionReaderTest = new PositionReaderTest(mPositionReader, mFileLen);
  }

  @After
  public void after() throws Exception {
    mPositionReader.close();
    mCacheManager.close();
    mLocalUfs.close();
    new File(mTestFile).delete();
  }

  @Test
  public void testAllCornerCases() throws IOException {
    mPositionReaderTest.testAllCornerCases();
  }

  @Test
  public void testReadRandomPart() throws IOException {
    mPositionReaderTest.testReadRandomPart();
  }

  @Test
  public void testConcurrentReadRandomPart() throws Exception {
    mPositionReaderTest.concurrentReadPart();
  }

  /**
   * Creates a manager and waits until it is ready.
   */
  private LocalCacheManager createLocalCacheManager(AlluxioConfiguration conf,
      PageMetaStore pageMetaStore) throws Exception {
    CacheManagerOptions cacheManagerOptions = CacheManagerOptions.create(conf);
    LocalCacheManager cacheManager = LocalCacheManager.create(cacheManagerOptions, pageMetaStore);
    CommonUtils.waitFor("restore completed",
        () -> cacheManager.state() == CacheManager.State.READ_WRITE,
        WaitForOptions.defaults().setTimeoutMs(10000));
    return cacheManager;
  }
}
