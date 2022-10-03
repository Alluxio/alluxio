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
import alluxio.client.file.cache.PageId;
import alluxio.sdk.file.cache.NativeCacheManager;
import alluxio.util.io.BufferUtils;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

@Fork(value = 1, jvmArgsPrepend = "-server")
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
public class NativeCacheBench {
  private static final int PAGE_SIZE_BYTES = Constants.MB;
  private static final int CACHE_SIZE_BYTES = 5 * Constants.GB;
  private static final byte[] PAGE1 = BufferUtils.getIncreasingByteArray(PAGE_SIZE_BYTES);

  @State(Scope.Benchmark)
  public static class BenchState {
    NativeCacheManager mCacheManager;

    public BenchState() {
      mCacheManager = new NativeCacheManager();
      mCacheManager.init(CACHE_SIZE_BYTES, 100 * PAGE_SIZE_BYTES, 16);
    }
  }

  @Benchmark
  @Measurement(iterations = 200, time = 20)
  @BenchmarkMode(Mode.All)
  public void putPageBench(BenchState state) {
    // byte[] buf = new byte[PAGE_SIZE_BYTES];
    //state.mStream.positionedRead(state.mRand.nextInt(100000000), buf, 0, PAGE_SIZE_BYTES);
    PageId pageId = new PageId(String.valueOf(System.nanoTime() / 100000), System.nanoTime() % 100);
    state.mCacheManager.put(pageId.toString().getBytes(), PAGE1);
  }
}
