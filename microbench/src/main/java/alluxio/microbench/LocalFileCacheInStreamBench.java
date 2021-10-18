/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.microbench;

import alluxio.Constants;
import alluxio.client.file.URIStatus;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManager.Factory;
import alluxio.client.file.cache.DefaultMetaStore;
import alluxio.client.file.cache.LocalCacheFileInStream;
import alluxio.client.file.cache.LocalCacheFileInStream.FileInStreamOpener;
import alluxio.client.file.cache.MetaStore;
import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.PageStore;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.hadoop.AlluxioHdfsInputStream;
import alluxio.util.CommonUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.WaitForOptions;
import alluxio.util.io.BufferUtils;
import alluxio.wire.FileInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

public class LocalFileCacheInStreamBench {
  private static final int PAGE_SIZE_BYTES = Constants.MB;
  private static final int CACHE_SIZE_BYTES = 2 * Constants.MB;
  private static final PageId PAGE_ID1 = new PageId("0L", 0L);
  private static final PageId PAGE_ID2 = new PageId("1L", 1L);
  private static final byte[] PAGE1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);
  private static final byte[] PAGE2 = BufferUtils.getIncreasingByteArray(255, PAGE_SIZE_BYTES);

  @State(Scope.Benchmark)
  public static class BenchState {
    CacheManager mCacheManager;
    private InstancedConfiguration mConf =
        new InstancedConfiguration(ConfigurationUtils.defaults());
    private MetaStore mMetaStore;
    private PageStore mPageStore;
    private CacheEvictor mEvictor;
    private PageStoreOptions mPageStoreOptions;
    LocalCacheFileInStream mStream;
    private byte[] mBuf = new byte[PAGE_SIZE_BYTES];
    private FileInStreamOpener mAlluxioFileOpener;
    Random rand = new Random();

    public BenchState() {
      mConf.set(PropertyKey.USER_CLIENT_CACHE_PAGE_SIZE, PAGE_SIZE_BYTES);
      mConf.set(PropertyKey.USER_CLIENT_CACHE_SIZE, CACHE_SIZE_BYTES);
      mConf.set(PropertyKey.USER_CLIENT_CACHE_DIR, "/tmp");
      mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_WRITE_ENABLED, false);
      mConf.set(PropertyKey.USER_CLIENT_CACHE_QUOTA_ENABLED, false);
      mConf.set(PropertyKey.USER_CLIENT_CACHE_STORE_OVERHEAD, 0);
      // default setting in prestodb
      mConf.set(PropertyKey.USER_CLIENT_CACHE_ASYNC_RESTORE_ENABLED, true);
      // default setting in prestodb
      mConf.set(PropertyKey.USER_CLIENT_CACHE_TIMEOUT_DURATION, "60s");
      mPageStoreOptions = PageStoreOptions.create(mConf);
      try {
        mPageStore = PageStore.create(mPageStoreOptions);
        mEvictor = new FIFOCacheEvictor(mConf);
        mMetaStore = new DefaultMetaStore(mEvictor);
        mCacheManager = Factory.get(this.mConf);
        CommonUtils.waitFor("restore completed",
            () -> mCacheManager.state() == CacheManager.State.READ_WRITE,
            WaitForOptions.defaults().setTimeoutMs(10000));
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop-master:9000");

        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        FileSystem fs = FileSystem.get(conf);
        String testFilename = "/tmp/mapreduce.tar.gz";
        Path testFilePath = new Path(testFilename);
        mAlluxioFileOpener = (status) -> new AlluxioHdfsInputStream(fs.open(testFilePath));
        FileInfo info = new FileInfo().setPath(testFilename).setFolder(false).setLength(100000000);

        // URIStatus uriStatus = new URIStatus(info, CacheContext.defaults());
        URIStatus uriStatus = new URIStatus(info);
        mStream = new LocalCacheFileInStream(uriStatus, mAlluxioFileOpener, mCacheManager, mConf);

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
    byte[] buf = new byte[PAGE_SIZE_BYTES];
    try {
      state.mStream.positionedRead(state.rand.nextInt(100000000), buf, 0, PAGE_SIZE_BYTES);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}
