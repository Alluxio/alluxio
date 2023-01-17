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
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import alluxio.Constants;
import alluxio.client.file.cache.CacheManager;
import alluxio.client.file.cache.CacheManagerOptions;
import alluxio.client.file.cache.evictor.CacheEvictorOptions;
import alluxio.client.file.cache.evictor.FIFOCacheEvictor;
import alluxio.client.file.cache.store.PageStoreDir;
import alluxio.client.file.cache.store.PageStoreOptions;
import alluxio.client.file.cache.store.PageStoreType;
import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.Configuration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.AlluxioStatusException;
import alluxio.master.NoopUfsManager;
import alluxio.util.CommonUtils;
import alluxio.underfs.UfsManager;
import alluxio.worker.block.BlockMasterClient;
import alluxio.worker.block.BlockMasterClientPool;
import alluxio.worker.block.BlockStoreEventListener;
import alluxio.worker.block.BlockStoreLocation;
import alluxio.worker.block.CreateBlockOptions;
import alluxio.worker.block.io.BlockWriter;

import com.google.common.collect.ImmutableList;
import io.grpc.Status;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

public class PagedBlockStoreCommitBlockTest {
  BlockStoreEventListener mListener;
  UfsManager mUfs;
  AlluxioConfiguration mConf;
  CacheManagerOptions mCacheManagerOptions;
  PagedBlockMetaStore mPageMetaStore;
  List<PagedBlockStoreDir> mDirs;
  PagedBlockStore mPagedBlockStore;
  BlockMasterClientPool mBlockMasterClientPool;
  BlockMasterClient mMockedBlockMasterClient;
  AtomicReference<Long> mWorkerId;

  CacheManager mCacheManager;

  private static final int DIR_INDEX = 0;

  private static final Long SESSION_ID = 1L;
  private static final Long BLOCK_ID = 2L;
  final int mBlockSize = 64;

  public int mPageSize = 2;

  private static final int OFFSET = 0;

  @Rule
  public TemporaryFolder mTempFolder = new TemporaryFolder();

  @Before
  public void setup() throws Exception {
    List<PageStoreDir> pageStoreDirs;
    InstancedConfiguration cacheManagerConf = Configuration.copyGlobal();

    Path dirPath = mTempFolder.newFolder().toPath();
    InstancedConfiguration dirConf = Configuration.modifiableGlobal();
    dirConf.set(PropertyKey.WORKER_PAGE_STORE_DIRS, ImmutableList.of(dirPath));
    dirConf.set(PropertyKey.WORKER_PAGE_STORE_SIZES, ImmutableList.of(Constants.MB));
    dirConf.set(PropertyKey.WORKER_PAGE_STORE_TYPE, PageStoreType.LOCAL);
    PageStoreDir pageStoreDir =
            PageStoreDir.createPageStoreDir(
                    new CacheEvictorOptions().setEvictorClass(FIFOCacheEvictor.class),
                    PageStoreOptions.createForWorkerPageStore(dirConf).get(DIR_INDEX));

    try  {
      mUfs = new NoopUfsManager();
      mConf = Configuration.global();
      cacheManagerConf.set(PropertyKey.WORKER_PAGE_STORE_PAGE_SIZE, mPageSize);
      cacheManagerConf.set(PropertyKey.WORKER_PAGE_STORE_DIRS, ImmutableList.of(dirPath));

      // Here mock BlockMasterClientPool and BlockMasterClient since I have no idea
      // about how to override them.
      // mockedPool will return a mocked BlockMasterClient when require() is called,
      // and do nothing when releasing, maybe add some action later on.
      mBlockMasterClientPool = mock(BlockMasterClientPool.class);
      mMockedBlockMasterClient = mock(BlockMasterClient.class);
      when(mBlockMasterClientPool.acquire()).thenReturn(mMockedBlockMasterClient);
      doNothing().when(mBlockMasterClientPool).release(any());
      mWorkerId = new AtomicReference<>(-1L);
      mCacheManagerOptions = CacheManagerOptions.createForWorker(cacheManagerConf);
      pageStoreDirs = new ArrayList<PageStoreDir>();
      pageStoreDirs.add(pageStoreDir);
      mDirs = PagedBlockStoreDir.fromPageStoreDirs(pageStoreDirs);
    } catch (Exception e) {
      throw e;
    }

    mListener = mock(BlockStoreEventListener.class);
    doAnswer((i) -> {
      assertEquals(BLOCK_ID, i.getArguments()[0]);
      return 0;
    }).when(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
    doAnswer((i) -> {
      assertEquals(BLOCK_ID, i.getArguments()[0]);
      return 0;
    }).when(mListener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
  }

  // This Test case success both to commit, no Exception should be thrown,
  // and both onCommit method should be called
  @Test
  public void localCommitAndMasterCommitBothSuccess() throws IOException, InterruptedException, TimeoutException {
    try {
      mPageMetaStore = new PagedBlockMetaStore(mDirs) {
        // here commit always success
        @Override
        public PagedBlockMeta commit(long BLOCK_ID) {
            return super.commit(BLOCK_ID);
        }
      };
      mCacheManager = CacheManager.Factory.create(mConf, mCacheManagerOptions, mPageMetaStore);

      mPagedBlockStore = new PagedBlockStore(mCacheManager, mUfs, mBlockMasterClientPool, mWorkerId,
              mPageMetaStore, mCacheManagerOptions.getPageSize());
    } catch (IOException e) {
      throw new RuntimeException();
    }

    try {
      prepareBlockStore();
    } catch (Exception e) {
      throw e;
    }

    mPagedBlockStore.commitBlock(SESSION_ID, BLOCK_ID, false);
    verify(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
    verify(mListener).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
  }

  // This Test case success commitToMaster, expecting one exception,

  @Test
  public void localCommitFailAndMasterCommitSuccess() throws IOException, InterruptedException, TimeoutException {
    try {
      mPageMetaStore = new PagedBlockMetaStore(mDirs) {
          // here commit always throw Exception
          @Override
          public PagedBlockMeta commit(long BLOCK_ID) {
              throw new RuntimeException();
          }
      };
      mCacheManager = CacheManager.Factory.create(mConf, mCacheManagerOptions, mPageMetaStore);

      mPagedBlockStore = new PagedBlockStore(mCacheManager, mUfs, mBlockMasterClientPool,
              mWorkerId, mPageMetaStore, mCacheManagerOptions.getPageSize());
    } catch (IOException e) {
      throw new RuntimeException();
    }

    try {
      prepareBlockStore();
    } catch (Exception e) {
      throw e;
    }

    assertThrows(RuntimeException.class, () -> {
      mPagedBlockStore.commitBlock(SESSION_ID, BLOCK_ID, false);
    });

    verify(mListener, never()).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
    verify(mListener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));

    try {
      mPagedBlockStore.close();
    } catch (Exception e) {
      throw e;
    }
  }

  // This Test case success commitToLocal, expecting one exception,
  // and only one onCommit method should be called.
  @Test
  public void localCommitSuccessAndMasterCommitFail() throws IOException, InterruptedException, TimeoutException {
    try {
      doAnswer((i) -> {
        throw new AlluxioStatusException(Status.UNAVAILABLE);
      }).when(mMockedBlockMasterClient).commitBlock(anyLong(), anyLong(), anyString(),
               anyString(), anyLong(), anyLong());
    } catch (AlluxioStatusException e) {
      throw new RuntimeException();
    }
    try {
      mPageMetaStore = new PagedBlockMetaStore(mDirs) {
        // here commit always success
        @Override
        public PagedBlockMeta commit(long BLOCK_ID) {
          return super.commit(BLOCK_ID);
        }
      };
      mCacheManager = CacheManager.Factory.create(mConf, mCacheManagerOptions, mPageMetaStore);

      mPagedBlockStore = new PagedBlockStore(mCacheManager, mUfs, mBlockMasterClientPool, mWorkerId,
               mPageMetaStore, mCacheManagerOptions.getPageSize());
    } catch (IOException e) {
      throw new RuntimeException();
    }

    try {
      prepareBlockStore();
    } catch (Exception e) {
      throw e;
    }

    assertThrows(RuntimeException.class, () -> {
      mPagedBlockStore.commitBlock(SESSION_ID, BLOCK_ID, false);
    });
    verify(mListener).onCommitBlockToLocal(anyLong(), any(BlockStoreLocation.class));
    verify(mListener, never()).onCommitBlockToMaster(anyLong(), any(BlockStoreLocation.class));
  }

  // Prepare PageBlockStore and creat a temp block for following test
  public void prepareBlockStore() throws IOException, InterruptedException, TimeoutException {
    PagedBlockStoreDir dir =
            (PagedBlockStoreDir) mPageMetaStore.allocate(BlockPageId.tempFileIdOf(BLOCK_ID), 1);

    dir.putTempFile(BlockPageId.tempFileIdOf(BLOCK_ID));
    PagedTempBlockMeta blockMeta = new PagedTempBlockMeta(BLOCK_ID, dir);
    mPagedBlockStore.createBlock(SESSION_ID, BLOCK_ID, OFFSET,
             new CreateBlockOptions(null, null, mBlockSize));
    byte[] data = new byte[mBlockSize];
    Arrays.fill(data, (byte) 1);
    ByteBuffer buf = ByteBuffer.wrap(data);
    try (BlockWriter writer = mPagedBlockStore.createBlockWriter(SESSION_ID, BLOCK_ID)) {
      CommonUtils.waitFor("writer initiation complete", () -> mPagedBlockStore.getCacheManagerState() == CacheManager.State.READ_WRITE);
      writer.append(buf);
    } catch (Exception e) {
      throw e;
    }

    mPagedBlockStore.registerBlockStoreEventListener(mListener);
  }
}
