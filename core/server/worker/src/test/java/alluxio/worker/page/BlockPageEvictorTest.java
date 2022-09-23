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

package alluxio.worker.page;

import static org.junit.Assert.assertEquals;

import alluxio.client.file.cache.PageId;
import alluxio.client.file.cache.evictor.CacheEvictor;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.evictor.LFUCacheEvictor;
import alluxio.client.file.cache.evictor.LRUCacheEvictor;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.util.CommonUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

@RunWith(Parameterized.class)
public class BlockPageEvictorTest {
  private static final int NUM_ACCESSES = 100000;
  @Parameterized.Parameter(0)
  public Class<? extends CacheEvictor> mEvictorType;

  @Parameterized.Parameter(1)
  public int mNumBlocks;

  @Parameterized.Parameter(2)
  public int mNumPages;

  @Parameterized.Parameter(3)
  public int mNumPinnedBlocks;

  private CacheEvictor mInner;

  private BlockPageEvictor mEvictor;

  @Parameterized.Parameters(
      name = "Evictor: {0}, Blocks: {1}, Pages: {2}, PinnedBlocks: {3}")
  public static Collection<Object[]> data() {
    List<Class<? extends CacheEvictor>> evictors = ImmutableList.of(
        LRUCacheEvictor.class,
        LFUCacheEvictor.class,
        FIFOCacheEvictor.class
    );
    List<Integer> numBlocks = ImmutableList.of(10, 20);
    List<Integer> numPages = ImmutableList.of(10, 20);
    List<Integer> numPinnedBlocks = ImmutableList.of(1, 2, 5);
    return Lists.cartesianProduct(evictors, numBlocks, numPages, numPinnedBlocks)
        .stream()
        .map(list -> list.toArray(new Object[0]))
        .collect(Collectors.toList());
  }

  @Before
  public void setup() {
    mInner = CommonUtils.createNewClassInstance(
        mEvictorType,
        new Class[] {AlluxioConfiguration.class},
        new Object[] {Configuration.global()});
    mEvictor = new BlockPageEvictor(mInner);
  }

  @Test
  public void noPinnedBlocks() {
    runAndInspect(NUM_ACCESSES, mNumBlocks, mNumPages, () -> mInner.evict());
  }

  @Test
  public void pinnedBlocks() {
    Set<Long> pinnedBlocks =
        LongStream.range(0, mNumPinnedBlocks).boxed().collect(Collectors.toSet());
    Set<String> pinnedBlockIdStr =
        pinnedBlocks.stream().map(String::valueOf).collect(Collectors.toSet());
    for (long blockId : pinnedBlocks) {
      mEvictor.addPinnedBlock(blockId);
    }
    runAndInspect(NUM_ACCESSES, mNumBlocks, mNumPages,
        () -> mInner.evictMatching(
            pageId -> !pinnedBlockIdStr.contains(pageId.getFileId())));

    for (long blockId : pinnedBlocks) {
      mEvictor.removePinnedBlock(blockId);
    }
    runAndInspect(NUM_ACCESSES, mNumBlocks, mNumPages, () -> mInner.evict());
  }

  private void runAndInspect(
      int numAccesses, int numBlocks, int numPages, Supplier<PageId> truthSupplier) {
    Stream<Access> accessSequence = randomAccessSequence(numAccesses, numBlocks, numPages);
    Iterator<Access> it = accessSequence.iterator();
    while (it.hasNext()) {
      Access access = it.next();
      switch (access.mType) {
        case GET:
          mEvictor.updateOnGet(access.mPage);
          break;
        case PUT:
          mEvictor.updateOnPut(access.mPage);
          break;
        case DELETE:
          mEvictor.updateOnDelete(access.mPage);
          break;
        default:
          // no-op
      }
      assertEquals(truthSupplier.get(), mEvictor.evict());
    }
  }

  private static Stream<Access> randomAccessSequence(int length, int numBlocks, int numPages) {
    Random rng = new Random();
    return IntStream.range(0, length)
        .mapToObj(i -> {
          PageId page = new PageId(String.valueOf(rng.nextInt(numBlocks)), rng.nextInt(numPages));
          return new Access(AccessType.of(rng.nextInt(AccessType.values().length)), page);
        });
  }

  private enum AccessType {
    PUT, GET, DELETE;

    static AccessType of(int discriminant) {
      switch (discriminant) {
        case 0:
          return PUT;
        case 1:
          return GET;
        case 2:
          return DELETE;
        default:
          throw new IllegalArgumentException("Unknown access type: " + discriminant);
      }
    }
  }

  private static class Access {
    AccessType mType;
    PageId mPage;

    Access(AccessType type, PageId pageId) {
      mType = type;
      mPage = pageId;
    }
  }
}
