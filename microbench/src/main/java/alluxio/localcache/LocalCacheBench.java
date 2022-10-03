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

package alluxio.localcache;

import alluxio.Constants;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManager.Factory;
import alluxio.client.file.cache.DefaultPageMetaStore;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.client.file.cache.LocalCacheFileInStream.FileInStreamOpener;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageMetaStore;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.MemoryPageStore;
import alluxio.client.file.cache.store.MemoryPageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.util.CommonUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;

import com.google.common.collect.ImmutableList;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
public class LocalCacheBench {
  private static final int PAGE_SIZE_BYTES = Constants.MB;
  private static final int CACHE_SIZE_BYTES = 5 * Constants.GB;
  private static final PageId PAGE_ID1 = new PageId("0L", 0L);
  private static final PageId PAGE_ID2 = new PageId("1L", 1L);
  private static final byte[] PAGE1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);

  @State(Scope.Benchmark)
  public static class BenchState {
    private MemoryPageStoreDir mPageStoreDir;
    CacheManager mCacheManager;
    private InstancedConfiguration mConf = alluxio.conf.Configuration.copyGlobal();
    private PageMetaStore mMetaStore;
    private PageStore mPageStore;
    private CacheEvictor mEvictor;
    private PageStoreOptions mPageStoreOptions;
    LocalCacheFileInStream mStream;
    //private byte[] mBuf = new byte[PAGE_SIZE_BYTES];
    private FileInStreamOpener mAlluxioFileOpener;
    Random mRand = new Random();

    public BenchState() {
      mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
      mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, String.valueOf(CACHE_SIZE_BYTES));
      mConf.set(PropertyKey.USER_CLIENT_CACHE_DIRS, "/tmp");
      mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, false);
      mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, false);
      mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, 0);
      mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_TYPE, PageStoreType.MEM);
      // default setting in prestodb
      mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, true);
      // default setting in prestodb
      mConf.set(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION, "0s");
      mPageStoreOptions = PageStoreOptions.create(mConf).get(0);
      try {
        mPageStore = PageStore.create(mPageStoreOptions);
        mEvictor = new FIFOCacheEvictor(mConf);
        mPageStoreDir =
            new MemoryPageStoreDir(mPageStoreOptions, (MemoryPageStore) mPageStore, mEvictor);
        mMetaStore = new DefaultPageMetaStore(ImmutableList.of(mPageStoreDir));
        mCacheManager = Factory.get(mConf);
        CommonUtils.waitFor("restore completed",
            () -> mCacheManager.state() == CacheManager.State.READ_WRITE,
            WaitForOptions.defaults().setTimeoutMs(10000));
        mCacheManager.put(PAGE_ID1, PAGE1);
      } catch (IOException e) {
        e.printStackTrace();
      } catch (InterruptedException e) {
        e.printStackTrace();
      } catch (TimeoutException e) {
        e.printStackTrace();
      }
    }
  }

  @Benchmark
  @Measurement(iterations = 200, time = 20)
  @BenchmarkMode(Mode.All)
  public void readInternalBench(BenchState state) {
    // byte[] buf = new byte[PAGE_SIZE_BYTES];
    //state.mStream.positionedRead(state.mRand.nextInt(100000000), buf, 0, PAGE_SIZE_BYTES);
    PageId pageId = new PageId(String.valueOf(System.nanoTime() / 100000), System.nanoTime() % 100);
    state.mCacheManager.put(pageId, PAGE1);
  }
}
